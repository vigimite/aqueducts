//! Integration tests for aqueducts schemas
//!
//! Tests backwards compatibility, serialization, and basic functionality.

use aqueducts_schemas::{Aqueduct, Field};
use std::fs;
use std::path::Path;

#[test]
fn test_backwards_compatibility() {
    // Test that old field names are still supported via aliases
    let old_format_json = r#"{
        "sources": [
            {
                "type": "File",
                "name": "test_data",
                "file_type": {
                    "type": "Csv",
                    "options": {
                        "has_header": true,
                        "delimiter": ","
                    }
                },
                "location": "./data.csv",
                "storage_options": {}
            }
        ],
        "stages": [],
        "destination": {
            "type": "Delta",
            "name": "output",
            "location": "./output",
            "write_mode": {
                "operation": "Append"
            },
            "storage_options": {},
            "partition_cols": [],
            "table_properties": {},
            "custom_metadata": {},
            "schema": []
        }
    }"#;

    let parsed: Aqueduct = serde_json::from_str(old_format_json).unwrap();
    assert_eq!(parsed.sources.len(), 1);
    assert!(parsed.destination.is_some());
}

#[test]
fn test_field_defaults() {
    // Test field without nullable should get default (true)
    let field_json = r#"{
        "name": "test_field",
        "type": "string"
    }"#;

    let field: Field = serde_json::from_str(field_json).unwrap();
    assert!(field.nullable); // Should get default
}

#[test]
fn test_version_default() {
    // Test that version gets default value when missing
    let config_json = r#"{
        "sources": [],
        "stages": []
    }"#;

    let parsed: Aqueduct = serde_json::from_str(config_json).unwrap();
    assert_eq!(parsed.version, "v2");
}

#[test]
fn test_pipeline_serialization_roundtrip() {
    // Test complete pipeline roundtrip serialization
    let pipeline = Aqueduct {
        version: "v2".to_string(),
        sources: vec![],
        stages: vec![],
        destination: None,
    };

    let json = serde_json::to_string(&pipeline).unwrap();
    let parsed: Aqueduct = serde_json::from_str(&json).unwrap();

    assert_eq!(pipeline.version, parsed.version);
    assert_eq!(pipeline.sources.len(), parsed.sources.len());
}

#[test]
fn test_example_pipeline_files() {
    // Test that all example pipeline files can be deserialized
    let examples_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples");

    if !examples_dir.exists() {
        return; // Skip if examples directory doesn't exist
    }

    for entry in fs::read_dir(examples_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // Skip non-pipeline files
        if !file_name.starts_with("aqueduct_pipeline") {
            continue;
        }

        let content =
            fs::read_to_string(&path).unwrap_or_else(|_| panic!("Failed to read file: {:?}", path));

        // Test deserialization based on file extension
        let _pipeline: Aqueduct =
            if path.extension().unwrap() == "yml" || path.extension().unwrap() == "yaml" {
                serde_yml::from_str(&content)
                    .unwrap_or_else(|e| panic!("Failed to parse YAML file {}: {}", file_name, e))
            } else if path.extension().unwrap() == "json" {
                serde_json::from_str(&content)
                    .unwrap_or_else(|e| panic!("Failed to parse JSON file {}: {}", file_name, e))
            } else {
                continue; // Skip non-YAML/JSON files
            };

        println!("Successfully parsed: {}", file_name);
    }
}
