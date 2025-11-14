use tracing_appender::{non_blocking, rolling};
use tracing_error::ErrorLayer;
use tracing_subscriber::{
    fmt, fmt::time::OffsetTime, layer::SubscriberExt,
    util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

use crate::LogLevel;

pub fn init_logger(
    log_level: LogLevel,
    log_file: String,
) -> anyhow::Result<non_blocking::WorkerGuard> {
    let filter = EnvFilter::from_default_env()
        .add_directive(format!("{}", log_level.to_string()).parse()?);

    let formatting_layer = fmt::layer()
        .pretty()
        .with_timer(OffsetTime::local_rfc_3339().expect("could not get local offset!"))
        .with_writer(std::io::stderr);

    let file_appender = rolling::daily("logs/", log_file.as_str()); // 每天一个日志文件
    let (non_blocking_appender, guard) = non_blocking(file_appender); // 输出非阻塞
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking_appender) // 文件输出日志等级
        .boxed();

    Registry::default()
        .with(filter)
        .with(ErrorLayer::default())
        .with(formatting_layer)
        .with(file_layer)
        .init();

    Ok(guard)
}
