//! Generate JSON schema for Aqueducts configuration
//!
//! This tool generates the JSON schema for the aqueducts-schemas crate and outputs it
//! to the json_schema directory for use in documentation.

use aqueducts_schemas::Aqueduct;
use schemars::schema_for;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema_for!(Aqueduct);
    let schema_json = serde_json::to_string_pretty(&schema)?;
    let output_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("json_schema")
        .join("aqueducts.schema.json");

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = File::create(&output_path)?;
    file.write_all(schema_json.as_bytes())?;

    println!("Generated JSON schema at: {}", output_path.display());

    Ok(())
}
