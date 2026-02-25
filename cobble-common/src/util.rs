use crate::Config;
use bytes::Bytes;

/// Creates a `Bytes` instance that shares the same underlying data as the input slice.
#[inline]
pub(crate) fn unsafe_bytes(target: &[u8]) -> Bytes {
    Bytes::from_owner(unsafe { std::slice::from_raw_parts(target.as_ptr(), target.len()) })
}

pub(crate) fn init_logging(config: &Config) {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let mut builder = log4rs::Config::builder();
        let mut root = log4rs::config::Root::builder();
        if config.log_console {
            let stdout = log4rs::append::console::ConsoleAppender::builder().build();
            builder = builder
                .appender(log4rs::config::Appender::builder().build("stdout", Box::new(stdout)));
            root = root.appender("stdout");
        }
        if let Some(ref path) = config.log_path {
            let file = log4rs::append::file::FileAppender::builder()
                .build(path)
                .unwrap();
            builder =
                builder.appender(log4rs::config::Appender::builder().build("file", Box::new(file)));
            root = root.appender("file");
        }
        let root = root.build(config.log_level);
        let config = builder.build(root).unwrap();
        let _ = log4rs::init_config(config);
    });
}
