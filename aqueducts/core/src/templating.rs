use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::{collections::HashMap, path::Path};

use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::{parser::DFParser, sqlparser::dialect::GenericDialect};
use miette::{Diagnostic, NamedSource, SourceOffset, SourceSpan};
use regex::Regex;
use tracing::debug;

use crate::Aqueduct;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum TemplateError {
    #[error("Aqueducts file not found: '{file_path}'")]
    #[diagnostic(
        code(aqueducts::template::file_not_found),
        help("Check that the file path is correct and the file exists")
    )]
    NotFound {
        #[source]
        source: std::io::Error,
        file_path: String,
    },

    #[error("Unsupported configuration format: '{0}'")]
    #[diagnostic(
        code(aqueducts::template::unknown_format),
        help(
            "The configuration format '{0}' is not supported in this build.\n\
             \n\
             Available formats in this build:\n\
             {}\n\
             \n\
             To enable additional formats, rebuild with the appropriate feature flag:\n\
             • For JSON: --features json\n\
             • For YAML: --features yaml\n\
             • For TOML: --features toml",
            Self::available_formats()
        )
    )]
    UnknownFormat(TemplateFormat),

    #[error("Missing template parameters: {0:?}")]
    #[diagnostic(
        code(aqueducts::template::missing_params),
        help(
            "Provide the missing parameters using the -p flag.\n\
              \n\
              Example:\n\
              aqueducts run -f pipeline.yaml -p param1=value1 -p param2=value2"
        )
    )]
    MissingParams(HashSet<String>),

    #[cfg(feature = "json")]
    #[error("JSON parsing error")]
    #[diagnostic(code(aqueducts::template::json_parse_error))]
    ParseJson {
        #[source_code]
        source_code: Arc<NamedSource<String>>,
        #[label("{}", error)]
        span: SourceSpan,
        #[source]
        error: serde_json::Error,
    },

    #[cfg(feature = "yaml")]
    #[error("YAML parsing error")]
    #[diagnostic(code(aqueducts::template::yaml_parse_error))]
    ParseYaml {
        #[source_code]
        source_code: Arc<NamedSource<String>>,
        #[label("{}", error)]
        span: SourceSpan,
        #[source]
        error: serde_yml::Error,
    },

    #[cfg(feature = "toml")]
    #[error(transparent)]
    #[diagnostic(code(aqueducts::template::toml_serialize_error))]
    ParseSerToml(#[from] toml::ser::Error),

    #[cfg(feature = "toml")]
    #[error("TOML parsing error")]
    #[diagnostic(code(aqueducts::template::toml_parse_error))]
    ParseDeToml {
        #[source_code]
        source_code: Arc<NamedSource<String>>,
        #[label("{}", error)]
        span: SourceSpan,
        #[source]
        error: toml::de::Error,
    },

    #[error("SQL syntax error in query")]
    #[diagnostic(
        code(aqueducts::template::sql_validation_error),
        help(
            "Check your SQL syntax in '{name}' query:\n\
              • Official docs: https://datafusion.apache.org/user-guide/sql/index.html"
        )
    )]
    SqlValidation {
        #[source_code]
        source_code: Arc<NamedSource<String>>,
        #[source]
        error: ParserError,
        #[label("syntax error in stage '{name}'")]
        span: SourceSpan,
        name: String,
    },
}

impl TemplateError {
    fn available_formats() -> String {
        let mut formats = vec![];

        #[cfg(feature = "json")]
        formats.push("• JSON (.json)");

        #[cfg(feature = "yaml")]
        formats.push("• YAML (.yaml, .yml)");

        #[cfg(feature = "toml")]
        formats.push("• TOML (.toml)");

        if formats.is_empty() {
            "No formats are currently enabled".to_string()
        } else {
            formats.join("\n")
        }
    }
}

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

impl std::fmt::Display for TemplateFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateFormat::Json => write!(f, "json"),
            TemplateFormat::Toml => write!(f, "toml"),
            TemplateFormat::Yaml => write!(f, "yaml"),
            TemplateFormat::Unknown(format) => write!(f, "{format}"),
        }
    }
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
    fn from_file<P: AsRef<Path>>(
        path: P,
        format: TemplateFormat,
        params: HashMap<String, String>,
    ) -> Result<Aqueduct, TemplateError>;

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
    ) -> Result<Aqueduct, TemplateError>;

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
    fn substitute_params(
        raw: &str,
        params: HashMap<String, String>,
    ) -> Result<String, TemplateError> {
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
            return Err(TemplateError::MissingParams(missing_params));
        }

        Ok(definition)
    }

    /// Validate SQL syntax in all stages within an Aqueducts pipeline
    /// Parse the SQL using DataFusion's parser with GenericDialect
    fn validate_sql(aqueduct: &Aqueduct) -> Result<(), TemplateError> {
        let dialect = GenericDialect {};

        for stages in aqueduct.stages.iter() {
            for stage in stages.iter() {
                DFParser::parse_sql_with_dialect(&stage.query, &dialect).map_err(|error| {
                    let span = if let Some((line, col)) =
                        Self::try_extract_error_position(&error.to_string())
                    {
                        let lines: Vec<&str> = stage.query.lines().collect();
                        let mut offset = 0;

                        // Add bytes for lines before the error line
                        for (i, &line_content) in lines.iter().enumerate() {
                            if (i + 1) < line as usize {
                                offset += line_content.len() + 1; // +1 for newline
                            } else {
                                break;
                            }
                        }

                        // Add column offset within the error line
                        offset += (col.saturating_sub(1)) as usize;

                        // Create a span for the error token (assume 1 character if we can't determine length)
                        SourceSpan::new(offset.into(), 1)
                    } else {
                        // Fallback: highlight entire query
                        SourceSpan::new(0.into(), stage.query.len())
                    };

                    TemplateError::SqlValidation {
                        source_code: Arc::new(NamedSource::new(
                            &stage.name,
                            stage.query.clone(),
                        )
                        .with_language("SQL")),
                        error,
                        span,
                        name: stage.name.clone(),
                    }
                })?;
            }
        }

        Ok(())
    }

    // FIXME: this is obviously ultra janky but sqlparser does not expose enough information here
    fn try_extract_error_position(error_msg: &str) -> Option<(u64, u64)> {
        // Look for patterns like "Line: 1, Column: 5" or "at Line: 2, Column: 10"
        static ERROR_REGEX: OnceLock<Regex> = OnceLock::new();
        let regex = ERROR_REGEX.get_or_init(|| {
            Regex::new(r"(?:at\s+)?Line:\s*(\d+),\s*Column:\s*(\d+)").expect("invalid regex")
        });

        if let Some(captures) = regex.captures(error_msg) {
            let line = captures.get(1)?.as_str().parse::<u64>().ok()?;
            let column = captures.get(2)?.as_str().parse::<u64>().ok()?;
            Some((line, column))
        } else {
            None
        }
    }
}

impl TemplateLoader for Aqueduct {
    fn from_file<T: AsRef<Path>>(
        path: T,
        format: TemplateFormat,
        params: HashMap<String, String>,
    ) -> Result<Aqueduct, TemplateError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|e| TemplateError::NotFound {
            source: e,
            file_path: path.display().to_string(),
        })?;
        Self::from_str(contents, format, params)
    }

    fn from_str<T: AsRef<str>>(
        value: T,
        format: TemplateFormat,
        params: HashMap<String, String>,
    ) -> Result<Aqueduct, TemplateError> {
        let contents = value.as_ref();

        debug!("Parsing template with format: {format:?}");

        let aqueduct = match format {
            TemplateFormat::Toml => {
                #[cfg(feature = "toml")]
                {
                    debug!("Parsing TOML string");
                    let parsed = toml::from_str::<toml::Value>(contents).map_err(|error| {
                        let offset = if let Some(span) = error.span() {
                            SourceOffset::from(span.start)
                        } else {
                            SourceOffset::from(0)
                        };
                        TemplateError::ParseDeToml {
                            source_code: Arc::new(NamedSource::new("pipeline.toml", contents.to_string())),
                            span: SourceSpan::new(offset, 1),
                            error,
                        }
                    })?;
                    let parsed = toml::to_string(&parsed).map_err(|error| {
                        // This is a serialization error, use from for now
                        TemplateError::ParseSerToml(error)
                    })?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    toml::from_str::<Aqueduct>(definition.as_str()).map_err(|error| {
                        let offset = if let Some(span) = error.span() {
                            SourceOffset::from(span.start)
                        } else {
                            SourceOffset::from(0)
                        };
                        TemplateError::ParseDeToml {
                            source_code: Arc::new(NamedSource::new("pipeline.toml", definition.clone())),
                            span: SourceSpan::new(offset, 1),
                            error,
                        }
                    })?
                }
                #[cfg(not(feature = "toml"))]
                {
                    return Err(TemplateError::UnknownFormat(TemplateFormat::Toml));
                }
            }
            TemplateFormat::Json => {
                #[cfg(feature = "json")]
                {
                    debug!("Parsing JSON string");
                    let parsed =
                        serde_json::from_str::<serde_json::Value>(contents).map_err(|error| {
                            let offset =
                                SourceOffset::from_location(contents, error.line(), error.column());
                            TemplateError::ParseJson {
                                source_code: Arc::new(NamedSource::new(
                                    "pipeline.json",
                                    contents.to_string(),
                                )),
                                span: SourceSpan::new(offset, 1),
                                error,
                            }
                        })?;
                    let parsed = serde_json::to_string(&parsed).map_err(|error| {
                        TemplateError::ParseJson {
                            source_code: Arc::new(NamedSource::new("pipeline.json", contents.to_string())),
                            span: SourceSpan::new(0.into(), contents.len()),
                            error,
                        }
                    })?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    serde_json::from_str::<Aqueduct>(definition.as_str()).map_err(|error| {
                        let offset =
                            SourceOffset::from_location(&definition, error.line(), error.column());
                        TemplateError::ParseJson {
                            source_code: Arc::new(NamedSource::new("pipeline.json", definition.clone())),
                            span: SourceSpan::new(offset, 1),
                            error,
                        }
                    })?
                }
                #[cfg(not(feature = "json"))]
                {
                    return Err(TemplateError::UnknownFormat(TemplateFormat::Json));
                }
            }
            TemplateFormat::Yaml => {
                #[cfg(feature = "yaml")]
                {
                    debug!("Parsing YAML string");
                    // Keep the original content for error reporting
                    let parsed =
                        serde_yml::from_str::<serde_yml::Value>(contents).map_err(|error| {
                            let offset = if let Some(location) = error.location() {
                                SourceOffset::from_location(
                                    contents,
                                    location.line(),
                                    location.column(),
                                )
                            } else {
                                SourceOffset::from(0)
                            };
                            TemplateError::ParseYaml {
                                source_code: Arc::new(NamedSource::new(
                                    "pipeline.yaml",
                                    contents.to_string(),
                                )),
                                span: SourceSpan::new(offset, 1),
                                error,
                            }
                        })?;
                    let parsed = serde_yml::to_string(&parsed).map_err(|error| {
                        TemplateError::ParseYaml {
                            source_code: Arc::new(NamedSource::new("pipeline.yaml", contents.to_string())),
                            span: SourceSpan::new(0.into(), contents.len()),
                            error,
                        }
                    })?;
                    let definition = Self::substitute_params(parsed.as_str(), params)?;
                    serde_yml::from_str::<Aqueduct>(definition.as_str()).map_err(|error| {
                        let offset = if let Some(location) = error.location() {
                            SourceOffset::from_location(
                                &definition,
                                location.line(),
                                location.column(),
                            )
                        } else {
                            SourceOffset::from(0)
                        };
                        TemplateError::ParseYaml {
                            source_code: Arc::new(NamedSource::new("pipeline.yaml", definition.clone())),
                            span: SourceSpan::new(offset, 1),
                            error,
                        }
                    })?
                }
                #[cfg(not(feature = "yaml"))]
                {
                    return Err(TemplateError::UnknownFormat(TemplateFormat::Yaml));
                }
            }
            fmt @ TemplateFormat::Unknown(_) => return Err(TemplateError::UnknownFormat(fmt)),
        };

        Self::validate_sql(&aqueduct)?;

        Ok(aqueduct)
    }
}

pub fn format_from_path<P: AsRef<Path>>(path: P) -> TemplateFormat {
    let path = path.as_ref();
    let ext = path.extension().and_then(|s| s.to_str());

    match ext {
        Some("toml") => TemplateFormat::Toml,
        Some("json") => TemplateFormat::Json,
        Some("yml") | Some("yaml") => TemplateFormat::Yaml,
        ext => TemplateFormat::Unknown(ext.unwrap_or("unknown_ext").to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sql_validation_valid_query() {
        let yaml_content = r#"
sources: []
stages:
  - - name: stage1
      query: SELECT 1 as id, 'test' as name
destinations:
  - name: output
    type: memory
"#;

        let result = Aqueduct::from_str(yaml_content, TemplateFormat::Yaml, HashMap::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_sql_validation_invalid_query() {
        let yaml_content = r#"
sources: []
stages:
  - - name: stage1
      query: SELECTT 1 as id, 'test' as name
destinations:
  - name: output
    type: memory
"#;

        let result = Aqueduct::from_str(yaml_content, TemplateFormat::Yaml, HashMap::new());
        assert!(result.is_err());

        if let Err(TemplateError::SqlValidation { source_code, .. }) = result {
            assert!(source_code.inner().contains("SELECTT"));
        } else {
            panic!("Expected SqlValidation error");
        }
    }

    #[test]
    fn test_sql_validation_multiple_stages() {
        let yaml_content = r#"
sources: []
stages:
  - - name: stage1
      query: SELECT 1 as id
  - - name: stage2
      query: SELECT id FROM stage1
  - - name: stage3
      query: INVALID SQL SYNTAX
destinations:
  - name: output
    type: memory
"#;

        let result = Aqueduct::from_str(yaml_content, TemplateFormat::Yaml, HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_validation_nested_valid_query() {
        let yaml_content = r#"
sources: []
stages:
  - - name: stage1
      query: |
        SELECT 
          id,
          name,
          CASE 
            WHEN id > 10 THEN 'high'
            ELSE 'low'
          END as category
        FROM (
          SELECT 1 as id, 'test' as name
          UNION ALL
          SELECT 2 as id, 'test2' as name
        ) t
        WHERE id > 0
        ORDER BY id
destinations:
  - name: output
    type: memory
"#;

        let result = Aqueduct::from_str(yaml_content, TemplateFormat::Yaml, HashMap::new());
        assert!(result.is_ok());
    }
}
