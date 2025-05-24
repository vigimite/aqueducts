//! Binary to generate JSON schema for the Aqueduct types
//!
//! This binary can be run with: cargo run --bin generate_schema --features schema_gen

use aqueducts_schemas::Aqueduct;
use schemars::schema_for;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate the JSON schema
    let schema = schema_for!(Aqueduct);

    // Serialize to pretty JSON
    let schema_json = serde_json::to_string_pretty(&schema)?;

    // Write to the json_schema directory in the project root
    let output_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("json_schema")
        .join("aqueducts.schema.json");

    // Ensure the output directory exists
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write the schema file
    let mut file = File::create(&output_path)?;
    file.write_all(schema_json.as_bytes())?;

    println!("Generated JSON schema at: {}", output_path.display());

    Ok(())
}
