use std::collections::HashSet;
use std::sync::OnceLock;
use std::{collections::HashMap, path::Path};

use regex::Regex;
use tracing::{debug, error};

use crate::error::AqueductsError;
use crate::error::Result;
use crate::Aqueduct;

/// Serialization format of the Aqueduct pipeline configuration.
///
/// Aqueducts supports multiple configuration file formats.
/// The format is typically inferred from the file extension, but can also be
/// specified explicitly when loading from strings.
#[derive(Debug, Clone)]
pub enum TemplateFormat {
    /// JSON format (.json files)
    Json,
    /// TOML format (.toml files)
    Toml,
    /// YAML format (.yml or .yaml files)
    Yaml,
    /// Unknown or unsupported format
    Unknown(String),
}

/// A trait for loading Aqueduct pipeline configurations from various sources with parameter substitution.
///
/// This trait provides the core functionality for loading pipeline configurations from files
/// or strings, with support for template parameter substitution. It handles multiple formats
/// (JSON, YAML, TOML) and validates that all required parameters are provided.
///
/// # Template Parameters
///
/// Aqueducts supports template parameters in configuration files using the `${parameter_name}` syntax.
/// These parameters are substituted at load time with values from the provided parameter map.
pub trait TemplateLoader {
    /// Load an Aqueduct pipeline configuration from a file.
    ///
    /// The file format is automatically inferred from the file extension:
    /// - `.json` → JSON format
    /// - `.toml` → TOML format  
    /// - `.yml` or `.yaml` → YAML format
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the configuration file
    /// * `params` - Template parameters to substitute in the configuration
    ///
    /// # Returns
    ///
    /// Returns the loaded pipeline configuration with all parameters substituted.
    ///
    /// # Errors
    ///
    /// - File not found or unreadable
    /// - Invalid file format or syntax
    /// - Missing required template parameters
    /// - Unsupported file extension
    fn from_file<T: AsRef<Path>>(path: T, params: HashMap<String, String>) -> Result<Aqueduct>;

    /// Load an Aqueduct pipeline configuration from a string.
    ///
    /// # Arguments
    ///
    /// * `value` - String containing the configuration
    /// * `format` - The format of the configuration string
    /// * `params` - Template parameters to substitute in the configuration
    ///
    /// # Returns
    ///
    /// Returns the loaded pipeline configuration with all parameters substituted.
    ///
    /// # Errors
    ///
    /// - Invalid configuration syntax
    /// - Missing required template parameters
    /// - Unsupported format
    fn from_str<T: AsRef<str>>(
        value: T,
        format: TemplateFormat,
        params: HashMap<String, String>,
    ) -> Result<Aqueduct>;

    /// Substitute template parameters in a configuration string.
    ///
    /// This method replaces all occurrences of `${parameter_name}` with the corresponding
    /// value from the provided parameter map. It validates that all template parameters
    /// in the string have corresponding values.
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw configuration string with template parameters
    /// * `params` - Map of parameter names to their values
    ///
    /// # Returns
    ///
    /// Returns the configuration string with all parameters substituted.
    ///
    /// # Errors
    ///
    /// Returns an error if any template parameters are missing from the parameter map.
    ///
    /// # Template Syntax
    ///
    /// Template parameters use the syntax `${parameter_name}` where `parameter_name`
    /// can contain letters, numbers, and underscores.
    fn substitute_params(raw: &str, params: HashMap<String, String>) -> Result<String> {
        static PARAM_REGEX: OnceLock<Regex> = OnceLock::new();
        let mut definition = raw.to_string();

        params.into_iter().for_each(|(name, value)| {
            let template = format!("${{{name}}}");
            definition = definition.replace(template.as_str(), value.as_str());
        });

        let captures = PARAM_REGEX
            .get_or_init(|| Regex::new("\\$\\{([a-zA-Z0-9_]+)\\}").expect("invalid regex"))
            .captures_iter(definition.as_str());

        let missing_params = captures
            .map(|capture| {
                let param = capture
                    .get(1)
                    .expect("no capture group found")
                    .as_str()
                    .to_string();
                param
            })
            .collect::<HashSet<String>>();

        if !missing_params.is_empty() {
            let error = AqueductsError::template(format!(
                "Missing template parameters: {:?}",
                missing_params
            ));

            error!("{error}");
            return Err(error);
        }

        Ok(definition)
    }
}

impl TemplateLoader for Aqueduct {
    fn from_file<T: AsRef<Path>>(path: T, params: HashMap<String, String>) -> Result<Aqueduct> {
        let path = path.as_ref();
        let ext = path.extension().and_then(|s| s.to_str()).ok_or_else(|| {
            AqueductsError::template(format!("Missing file extension for: {:?}", path))
        })?;

        debug!("Parsing file with extension: {}", ext);

        let format = match ext {
            "toml" => TemplateFormat::Toml,
            "json" => TemplateFormat::Json,
            "yml" | "yaml" => TemplateFormat::Yaml,
            ext => TemplateFormat::Unknown(ext.to_string()),
        };

        let contents = std::fs::read_to_string(path)?;
        Self::from_str(contents, format, params)
    }

    fn from_str<T: AsRef<str>>(
        value: T,
        format: TemplateFormat,
        params: HashMap<String, String>,
    ) -> Result<Aqueduct> {
        let contents = value.as_ref();

        debug!("Parsing template with format: {format:?}");

        match format {
            TemplateFormat::Toml => {
                #[cfg(feature = "toml")]
                {
                    debug!("Parsing TOML string");
                    let parsed = toml::from_str::<toml::Value>(contents)?;
                    let parsed = toml::to_string(&parsed)?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    let aqueduct = toml::from_str::<Aqueduct>(definition.as_str())?;

                    Ok(aqueduct)
                }
                #[cfg(not(feature = "toml"))]
                {
                    Err(AqueductsError::unsupported("template format", format!("{:?} support is not enabled in this build. Enable the corresponding feature flag", format)))
                }
            }
            TemplateFormat::Json => {
                #[cfg(feature = "json")]
                {
                    debug!("Parsing JSON string");
                    let parsed = serde_json::from_str::<serde_json::Value>(contents)?;
                    let parsed = serde_json::to_string(&parsed)?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

                    Ok(aqueduct)
                }
                #[cfg(not(feature = "json"))]
                {
                    Err(AqueductsError::unsupported("template format", format!("{:?} support is not enabled in this build. Enable the corresponding feature flag", format)))
                }
            }
            TemplateFormat::Yaml => {
                #[cfg(feature = "yaml")]
                {
                    debug!("Parsing YAML string");
                    let parsed = serde_yml::from_str::<serde_yml::Value>(contents)?;
                    let parsed = serde_yml::to_string(&parsed)?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

                    Ok(aqueduct)
                }
                #[cfg(not(feature = "yaml"))]
                {
                    Err(AqueductsError::unsupported("template format", format!("{:?} support is not enabled in this build. Enable the corresponding feature flag", format)))
                }
            }
            fmt @ TemplateFormat::Unknown(_) => Err(AqueductsError::unsupported("template format", format!("{:?} support is not enabled in this build. Enable the corresponding feature flag", fmt))),
        }
    }
}
