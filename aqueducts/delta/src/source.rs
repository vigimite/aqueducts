use aqueducts_schemas::DeltaSource;
use datafusion::execution::context::SessionContext;
use deltalake::DeltaTableBuilder;
use std::sync::Arc;

use crate::error::DeltaError;
use crate::handlers::register_handlers;

/// Register a Delta table as a source in the SessionContext.
///
/// This function allows reading from Delta tables with optional time travel
/// and automatic object store registration for cloud storage.
pub async fn register_delta_source(
    ctx: Arc<SessionContext>,
    config: &DeltaSource,
) -> std::result::Result<(), DeltaError> {
    // Ensure Delta Lake handlers are registered
    register_handlers();

    tracing::debug!(
        "Registering delta source '{}' at location '{}'",
        config.name,
        config.location
    );

    let builder = DeltaTableBuilder::from_valid_uri(config.location.as_str())?
        .with_storage_options(config.storage_config.clone());

    let table = if let Some(version) = config.version {
        builder.with_version(version).load().await?
    } else if let Some(timestamp) = &config.timestamp {
        builder.with_datestring(timestamp.clone())?.load().await?
    } else {
        builder.load().await?
    };

    ctx.register_table(&config.name, Arc::new(table))?;

    tracing::debug!("Successfully registered delta source '{}'", config.name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use aqueducts_schemas::DeltaSource;
    use std::collections::HashMap;
    use url::Url;

    #[test]
    fn test_delta_source_config_serialization() {
        let config = DeltaSource {
            name: "test_table".to_string(),
            location: aqueducts_schemas::Location(Url::parse("file:///tmp/delta-table").unwrap()),
            version: None,
            timestamp: None,
            storage_config: HashMap::new(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: DeltaSource = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.location, deserialized.location);
        assert_eq!(config.version, deserialized.version);
        assert_eq!(config.timestamp, deserialized.timestamp);
    }

    // Note: Integration tests with actual Delta tables would require
    // setting up test Delta tables, which is beyond the scope of unit tests
}
