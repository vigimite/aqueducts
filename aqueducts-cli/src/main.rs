use aqueducts::prelude::*;
use clap::Parser;
use std::{collections::HashMap, error::Error};

#[derive(Debug, Parser)]
struct Args {
    /// path to Aqueduct configuration file
    #[arg(short, long)]
    file: String,
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    aqueducts::register_handlers();

    let Args { file, params } = Args::parse();
    let params = HashMap::from_iter(params.unwrap_or_default());
    let aqueduct = Aqueduct::try_from_yml(file, params)?;

    run_pipeline(aqueduct, None).await?;

    Ok(())
}
