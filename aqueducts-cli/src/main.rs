use anyhow::Context;
use aqueducts::prelude::*;
use clap::Parser;
use env_logger::Env;
use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc};

#[derive(Debug, Parser)]
struct Args {
    /// path to Aqueduct configuration file
    #[arg(short, long)]
    file: PathBuf,
    /// k=v list of parameters to pass to the configuration file e.g. aqueduct -f file.yml -p key1=value1 -p key2=value2
    #[arg(short, long, value_parser = parse_key_val::<String, String>)]
    params: Option<Vec<(String, String)>>,
}

fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let env = Env::default().default_filter_or("aqueducts=info");
    env_logger::Builder::from_env(env)
        .format_target(false)
        .format_level(false)
        .format_timestamp(None)
        .init();

    aqueducts::register_handlers();

    let Args { file, params } = Args::parse();
    let params = HashMap::from_iter(params.unwrap_or_default());

    let aqueduct = match file.extension().and_then(|s| s.to_str()) {
        #[cfg(feature = "toml")]
        Some("toml") => {
            Aqueduct::try_from_toml(file, params).context("failed to parse provided file")?
        }
        #[cfg(feature = "json")]
        Some("json") => {
            Aqueduct::try_from_json(file, params).context("failed to parse provided file")?
        }
        _ => Aqueduct::try_from_yml(file, params).context("failed to parse provided file")?,
    };

    let ctx = datafusion::prelude::SessionContext::new();
    // datafusion_functions_json::register_all(&mut ctx).expect("failed to register json functions");

    run_pipeline(Arc::new(ctx), aqueduct)
        .await
        .context("failure during execution of aqueducts file")?;

    Ok(())
}
