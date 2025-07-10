//! Location type that handles both file paths and URLs

use serde::{Deserialize, Deserializer, Serialize};
use std::path::Path;
use url::Url;

/// A location that can be either a file path or a URL
///
/// This type automatically converts file paths to file:// URLs during deserialization
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::Location;
///
/// // From URL string
/// let url_location: Location = "https://example.com/data.csv".try_into().unwrap();
///
/// // From absolute file path
/// let file_location: Location = "/tmp/data.csv".try_into().unwrap();
///
/// // From relative file path
/// let rel_location: Location = "./data.csv".try_into().unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schema_gen",
    schemars(
        with = "String",
        description = "A file path or URL. File paths will be converted to file:// URLs. Examples: '/tmp/data.csv', './data.csv', 'https://example.com/data.csv', 's3://bucket/data.csv'"
    )
)]
pub struct Location(pub Url);

impl TryFrom<&str> for Location {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        // Try as URL first - if it has a scheme, it should parse as URL
        if let Ok(url) = Url::parse(s) {
            return Ok(Location(url));
        }

        // Try as file path
        let path = Path::new(s);
        let url = if path.is_absolute() {
            Url::from_file_path(path)
        } else {
            // For relative paths, resolve against current directory
            let current_dir = std::env::current_dir()
                .map_err(|e| format!("Cannot get current directory: {e}"))?;
            Url::from_file_path(current_dir.join(path))
        }
        .map_err(|_| format!("Invalid path: {s}"))?;

        Ok(Location(url))
    }
}

impl TryFrom<String> for Location {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Location::try_from(s.as_str())
    }
}

impl From<Url> for Location {
    fn from(url: Url) -> Self {
        Location(url)
    }
}

impl<'de> Deserialize<'de> for Location {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Location::try_from(s.as_str()).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<Url> for Location {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

impl std::ops::Deref for Location {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_parsing() {
        let location: Location = "https://example.com/data.csv".try_into().unwrap();
        assert_eq!(location.scheme(), "https");
        assert_eq!(location.host_str(), Some("example.com"));
    }

    #[test]
    fn test_absolute_file_path() {
        let location: Location = "/tmp/data.csv".try_into().unwrap();
        assert_eq!(location.scheme(), "file");
        assert!(location.path().ends_with("/tmp/data.csv"));
    }

    #[test]
    fn test_relative_file_path() {
        let location: Location = "./data.csv".try_into().unwrap();
        assert_eq!(location.scheme(), "file");
        assert!(location.path().ends_with("/data.csv"));
    }

    #[test]
    fn test_windows_path() {
        if cfg!(windows) {
            let location: Location = r"C:\temp\data.csv".try_into().unwrap();
            assert_eq!(location.scheme(), "file");
        }
    }

    #[test]
    fn test_s3_url() {
        let location: Location = "s3://my-bucket/data.csv".try_into().unwrap();
        assert_eq!(location.scheme(), "s3");
        assert_eq!(location.host_str(), Some("my-bucket"));
        assert_eq!(location.path(), "/data.csv");
    }

    #[test]
    fn test_serialization() {
        let location: Location = "https://example.com/data.csv".try_into().unwrap();
        let json = serde_json::to_string(&location).unwrap();
        assert_eq!(json, r#""https://example.com/data.csv""#);
    }

    #[test]
    fn test_deserialization() {
        let json = r#""./data.csv""#;
        let location: Location = serde_json::from_str(json).unwrap();
        assert_eq!(location.scheme(), "file");
        assert!(location.path().ends_with("/data.csv"));
    }

    #[test]
    fn test_location_in_config() {
        use serde_json;

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct Config {
            name: String,
            location: Location,
        }

        let config = Config {
            name: "test".to_string(),
            location: "s3://my-bucket/data".try_into().unwrap(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: Config = serde_json::from_str(&json).unwrap();

        assert_eq!(config.name, parsed.name);
        assert_eq!(config.location.as_ref(), parsed.location.as_ref());
    }

    #[test]
    fn test_mixed_location_types() {
        let locations = vec![
            ("./local.csv", "file"),
            ("/absolute/path.json", "file"),
            ("https://example.com/data.csv", "https"),
            ("s3://bucket/key.parquet", "s3"),
            ("gs://bucket/object", "gs"),
            ("azure://container/blob", "azure"),
        ];

        for (input, expected_scheme) in locations {
            let location = Location::try_from(input).unwrap();
            assert_eq!(
                location.scheme(),
                expected_scheme,
                "Failed for input: {input}"
            );
        }
    }

    #[test]
    fn test_yaml_deserialization() {
        let yaml = r#"
location: "./data/input.csv"
"#;

        #[derive(serde::Deserialize)]
        struct Config {
            location: Location,
        }

        let config: Config = serde_yml::from_str(yaml).unwrap();
        assert_eq!(config.location.scheme(), "file");
        assert!(config.location.path().ends_with("/data/input.csv"));
    }
}
