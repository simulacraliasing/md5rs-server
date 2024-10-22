use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Result};
use crossbeam_channel::{Receiver, Sender};
use image::ImageReader;
use ndarray::{s, Array3, Array4, Axis, Dim};
use nshare::AsNdarray3Mut;
use ort::{inputs, ExecutionProviderDispatch, Session, SessionOutputs};
use tracing::info;

use crate::{Bbox, DecodeTask, DetectResponse, DetectTask};

fn decode_webp(data: Vec<u8>, width: i32, height: i32) -> Result<(Array4<f32>, f32, i32)> {
    let cursor = Cursor::new(data);

    let image = ImageReader::new(cursor).with_guessed_format()?.decode()?;

    let mut image = image.to_rgb8();

    let image_array = image.as_ndarray3_mut().mapv(|x| x as f32 / 255.0);

    let imgsz = image_array.shape()[1].max(image_array.shape()[2]) as u32;

    let ratio = imgsz as f32 / width.max(height) as f32;

    let pad_width = (imgsz - image_array.shape()[2] as u32) / 2;

    let pad_height = (imgsz - image_array.shape()[1] as u32) / 2;

    let pad = pad_width as i32 - pad_height as i32;

    let mut padded_array = Array3::<f32>::from_elem(Dim([3, imgsz as usize, imgsz as usize]), 0.44);

    padded_array
        .slice_mut(s![
            ..,
            pad_height as i32..(imgsz - pad_height) as i32,
            pad_width as i32..(imgsz - pad_width) as i32
        ])
        .assign(&image_array);

    let padded_array = padded_array.insert_axis(Axis(0));

    Ok((padded_array, ratio, pad))
}

fn load_model(model_path: &Path, ep: ExecutionProviderDispatch) -> Result<Session> {
    let model = Session::builder()?
        .with_execution_providers([ep])?
        .commit_from_file(model_path)?;

    Ok(model)
}

impl Bbox {
    fn area(&self) -> f32 {
        (self.x2 - self.x1) * (self.y2 - self.y1)
    }
}

fn iou(box1: &Bbox, box2: &Bbox) -> f32 {
    let x1 = box1.x1.max(box2.x1);
    let y1 = box1.y1.max(box2.y1);
    let x2 = box1.x2.min(box2.x2);
    let y2 = box1.y2.min(box2.y2);

    let intersection_area = ((x2 - x1).max(0.0)) * ((y2 - y1).max(0.0));
    let union_area = box1.area() + box2.area() - intersection_area;

    if union_area == 0.0 {
        0.0
    } else {
        intersection_area / union_area
    }
}

fn nms(boxes: &mut Vec<Bbox>, agnostic: bool, topk: usize, iou_threshold: f32) -> Vec<Bbox> {
    // Sort boxes by score in descending order
    boxes.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

    let mut result = Vec::new();

    if agnostic {
        // Perform agnostic NMS
        while !boxes.is_empty() {
            let best_box = boxes.remove(0);
            result.push(best_box.clone());

            if result.len() >= topk {
                break;
            }

            boxes.retain(|b| iou(&best_box, b) < iou_threshold);
        }
    } else {
        // Perform class-specific NMS
        let mut class_map: std::collections::HashMap<usize, Vec<Bbox>> =
            std::collections::HashMap::new();

        for b in boxes.clone() {
            class_map
                .entry(b.class as usize)
                .or_insert_with(Vec::new)
                .push(b);
        }

        for (_, mut class_boxes) in class_map {
            while !class_boxes.is_empty() {
                let best_box = class_boxes.remove(0);
                result.push(best_box.clone());

                if result.iter().filter(|b| b.class == best_box.class).count() >= topk {
                    break;
                }

                class_boxes.retain(|b| iou(&best_box, b) < iou_threshold);
            }
        }
    }

    result
}

fn process_frame(
    uuid: String,
    image_array: Array4<f32>,
    ratio: f32,
    pad: i32,
    width: i32,
    height: i32,
    iou: f32,
    score: f32,
    class_map: &HashMap<usize, String>,
    model: &Session,
) -> Result<DetectResponse> {
    let outputs: SessionOutputs = model.run(inputs!["images" => image_array.view()]?)?;

    let output = outputs["output0"]
        .try_extract_tensor::<f32>()?
        .t()
        .into_owned();

    let output = output.slice(s![.., .., 0]);

    let mut bboxs = Vec::new();

    for row in output.axis_iter(Axis(1)) {
        let row: Vec<_> = row.iter().copied().collect();
        let class_id = row[5] as usize;
        let prob = row[4];
        if prob < score {
            continue;
        }
        let x1: f32;
        let y1: f32;
        let x2: f32;
        let y2: f32;

        if pad >= 0 {
            x1 = (row[0] / ratio) - pad as f32;
            y1 = (row[1] / ratio) as f32;
            x2 = (row[2] / ratio) - pad as f32;
            y2 = (row[3] / ratio) as f32;
        } else {
            x1 = (row[0] / ratio) as f32;
            y1 = (row[1] / ratio) + pad as f32;
            x2 = (row[2] / ratio) as f32;
            y2 = (row[3] / ratio) + pad as f32;
        }
        let bbox = Bbox {
            x1: x1.max(0.0),
            y1: y1.max(0.0),
            x2: x2.min(width as f32),
            y2: y2.min(height as f32),
            class: class_id as i32,
            score: prob,
        };
        bboxs.push(bbox);
    }

    bboxs = nms(&mut bboxs, true, 100, iou);

    let labels = get_label(&bboxs, class_map);

    Ok(DetectResponse {
        uuid,
        bboxs,
        label: labels,
    })
}

pub fn decode_worker(
    receiver: Arc<Receiver<DecodeTask>>,
    sender: Arc<Sender<DetectTask>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(task) = receiver.recv() {
            let (result, ratio, pad) =
                decode_webp(task.image_data, task.width, task.height).unwrap();
            let _ = sender.send(DetectTask {
                uuid: task.uuid,
                image_array: result,
                ratio,
                pad,
                width: task.width,
                height: task.height,
                iou: task.iou,
                score: task.score,
                response_sender: task.response_sender,
            });
        }
    })
}

pub fn detect_worker(receiver: Arc<Receiver<DetectTask>>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let ep = ort::TensorRTExecutionProvider::default()
            .with_engine_cache(true)
            .with_engine_cache_path("./models")
            .with_timing_cache(true)
            .with_fp16(true)
            .with_profile_min_shapes("images:1x3x1280x1280")
            .with_profile_opt_shapes("images:2x3x1280x1280")
            .with_profile_max_shapes("images:5x3x1280x1280")
            .with_device_id(3)
            .build();
        let model = load_model(Path::new("models/md_v5a_d_pp_fp16.onnx"), ep).unwrap();
        info!(
            "Model md_v5a_d_pp_fp16 loaded at {:?}",
            std::thread::current().id()
        );
        let class_map = [
            (0, "Animal".to_string()),
            (1, "Person".to_string()),
            (2, "Vehicle".to_string()),
        ]
        .iter()
        .cloned()
        .collect();
        while let Ok(task) = receiver.recv() {
            let result = process_frame(
                task.uuid,
                task.image_array,
                task.ratio,
                task.pad,
                task.width,
                task.height,
                task.iou,
                task.score,
                &class_map,
                &model,
            )
            .unwrap();
            let _ = task.response_sender.send(result);
        }
    })
}

fn get_label(bboxes: &Vec<Bbox>, cls_map: &HashMap<usize, String>) -> Vec<String> {
    let mut labels = HashSet::new();
    if bboxes.is_empty() {
        labels.insert("Blank".to_string());
        return labels.into_iter().collect();
    }

    for bbox in bboxes {
        let class_id = bbox.class as usize;

        let label = match cls_map.get(&class_id) {
            Some(label) => label.to_string(),
            None => Err(anyhow!("Class ID not found")).unwrap(),
        };

        labels.insert(label);
    }
    labels.into_iter().collect()
}
