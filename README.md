# A Megadetector gRPC server

This is a simple gRPC server built on [tonic](https://github.com/hyperium/tonic) to interact with [md5rs-client](https://github.com/simulacraliasing/md5rs-client).

## What are done by client side?

- Image and video decoding
- Frame preprocessing(resize)
- Encoding frame to webp and sending to server
- Receiving detection results from server
- Exporting detection results to json/csv file

## What are done by server side?

- Authentication
- Receiving webp encoded frames and decoding
- Frame preprocessing(padding)
- Inferencing
- Postprocessing including NMS
- Returning detection results to client