pub(crate) struct Config {
    database_url: Option<String>,
}

impl Config {
    pub(crate) fn load() -> Self {
        Self {
            database_url: Some("temp".into()),
        }
    }
}
