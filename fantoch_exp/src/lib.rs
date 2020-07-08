#![deny(rust_2018_idioms)]

#[cfg(feature = "exp")]
pub mod bench;
#[cfg(feature = "exp")]
pub mod exp;
#[cfg(feature = "exp")]
pub mod testbed;
#[cfg(feature = "exp")]
pub mod util;

pub mod config;

// Re-export `ExperimentConfig`.
pub use config::ExperimentConfig;

use color_eyre::eyre::WrapErr;
use color_eyre::Report;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::path::Path;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum RunMode {
    Release,
    Flamegraph,
}

impl RunMode {
    pub fn binary(&self, binary: &str) -> String {
        let binary = format!("./fantoch/target/release/{}", binary);
        match self {
            RunMode::Release => binary,
            RunMode::Flamegraph => {
                // `source` is needed in order for `flamegraph` to be found
                format!("source ~/.cargo/env && flamegraph {}", binary)
            }
        }
    }

    pub fn is_flamegraph(&self) -> bool {
        self == &RunMode::Flamegraph
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum FantochFeature {
    Amortize,
    Timing,
}

impl FantochFeature {
    pub fn name(&self) -> String {
        match self {
            FantochFeature::Amortize => "amortize",
            FantochFeature::Timing => "timing",
        }
        .to_string()
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    Hash,
)]
pub enum Protocol {
    AtlasLocked,
    EPaxosLocked,
    FPaxos,
    NewtAtomic,
    NewtLocked,
    Basic,
}

impl Protocol {
    pub fn binary(&self) -> &str {
        match self {
            Protocol::AtlasLocked => "atlas_locked",
            Protocol::EPaxosLocked => "epaxos_locked",
            Protocol::FPaxos => "fpaxos",
            Protocol::NewtAtomic => "newt_atomic",
            Protocol::NewtLocked => "newt_locked",
            Protocol::Basic => "basic",
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Testbed {
    Aws,
    Baremetal,
}

impl Testbed {
    pub fn is_aws(&self) -> bool {
        self == &Testbed::Aws
    }
}

pub enum SerializationFormat {
    Bincode,
    Json,
}

// TODO maybe make this async
pub fn serialize<T>(
    data: T,
    file: impl AsRef<Path>,
    format: SerializationFormat,
) -> Result<(), Report>
where
    T: serde::Serialize,
{
    // if the file does not exist it will be created, otherwise truncated
    let file = std::fs::File::create(file).wrap_err("serialize create file")?;
    // create a buf writer
    let buf = std::io::BufWriter::new(file);
    // and try to serialize
    match format {
        SerializationFormat::Bincode => {
            bincode::serialize_into(buf, &data).wrap_err("serialize")?
        }
        SerializationFormat::Json => {
            serde_json::to_writer(buf, &data).wrap_err("serialize")?
        }
    }
    Ok(())
}

// TODO maybe make this async
pub fn deserialize<T>(
    file: impl AsRef<Path>,
    format: SerializationFormat,
) -> Result<T, Report>
where
    T: serde::de::DeserializeOwned,
{
    // open the file in read-only
    let file = std::fs::File::open(file).wrap_err("deserialize open file")?;
    // create a buf reader
    let buf = std::io::BufReader::new(file);
    // and try to deserialize
    let data = match format {
        SerializationFormat::Bincode => {
            bincode::deserialize_from(buf).wrap_err("deserialize")?
        }
        SerializationFormat::Json => {
            serde_json::from_reader(buf).wrap_err("deserialize")?
        }
    };
    Ok(data)
}
