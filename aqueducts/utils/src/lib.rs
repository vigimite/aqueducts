/// custom serde
pub mod serde {
    use serde::{Deserialize, Deserializer};
    use std::path::Path;
    use url::{ParseError, Url};

    /// try to deserialize URL
    /// if URL deserialization fails due to it being a relative path this function will fallback to using the `std::path` API to create a canonical representation of the given path and then parse as a URL
    pub fn deserialize_file_location<'de, D>(deserializer: D) -> core::result::Result<Url, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;

        let url = match Url::parse(buf.as_str()) {
            Err(ParseError::RelativeUrlWithoutBase)
            | Err(ParseError::RelativeUrlWithCannotBeABaseBase)
                if buf.ends_with('/') || buf.ends_with('\\') =>
            {
                let canonicalized = Path::new(buf.as_str()).canonicalize().map_err(|e| {
                    serde::de::Error::custom(format!("error canonicalizing dir path: {e:?}"))
                })?;

                let url = Url::from_directory_path(canonicalized).map_err(|e| {
                    serde::de::Error::custom(format!("error parsing directory path as url: {e:?}"))
                })?;

                Ok(url)
            }
            Err(ParseError::RelativeUrlWithoutBase)
            | Err(ParseError::RelativeUrlWithCannotBeABaseBase) => {
                let (path, file_name) = buf
                    .contains('/')
                    .then_some('/')
                    .or(buf.contains('\\').then_some('\\'))
                    .and_then(|split_char| buf.as_str().rsplit_once(split_char))
                    .ok_or_else(|| {
                        serde::de::Error::custom(
                            "relative paths cannot only contain the file name".to_string(),
                        )
                    })?;

                // file might not exist in the output case
                let canonicalized = {
                    let mut path = Path::new(path).canonicalize().map_err(|e| {
                        serde::de::Error::custom(format!(
                            "error canonicalizing file path '{buf}': {e:?}"
                        ))
                    })?;

                    path.push(file_name);
                    path
                };

                let url = Url::from_file_path(canonicalized).map_err(|e| {
                    serde::de::Error::custom(format!(
                        "error parsing file path as url '{buf}': {e:?}"
                    ))
                })?;

                Ok(url)
            }
            Err(err) => Err(serde::de::Error::custom(format!(
                "error parsing location: {err:?}"
            ))),
            Ok(url) => Ok(url),
        }?;

        Ok(url)
    }
}

/// object store handlers
pub mod store {
    use deltalake::{
        datafusion::prelude::SessionContext, storage::StorageOptions, DeltaTableError,
    };
    use std::{collections::HashMap, sync::Arc};
    use url::Url;

    /// register deltalake object store handlers
    #[allow(dead_code)]
    pub fn register_handlers() {
        #[cfg(feature = "s3")]
        {
            deltalake::aws::register_handlers(None);
        }

        #[cfg(feature = "gcs")]
        {
            deltalake::gcp::register_handlers(None);
        }

        #[cfg(feature = "azure")]
        {
            deltalake::azure::register_handlers(None);
        }
    }

    pub fn register_object_store(
        ctx: Arc<SessionContext>,
        location: &Url,
        storage_options: &HashMap<String, String>,
    ) -> Result<(), DeltaTableError> {
        if location.scheme() == "file" || location.scheme() == "memory" {
            return Ok(());
        }

        let scheme = Url::parse(&format!("{}://", location.scheme())).unwrap();
        if let Some(factory) = deltalake::storage::factories().get(&scheme) {
            let (store, _prefix) =
                factory.parse_url_opts(location, &StorageOptions(storage_options.clone()))?;
            let _ = ctx
                .runtime_env()
                .register_object_store(location, Arc::new(store));

            Ok(())
        } else {
            Err(DeltaTableError::InvalidTableLocation(
                location.clone().into(),
            ))
        }
    }
}
