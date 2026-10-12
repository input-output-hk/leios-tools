//! `txflow` command line.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use txflow::context::ChainContext;
use txflow::error::{in_file, on_file, read_file, Error, Result};
use txflow::spec::ExperimentSpec;
use txflow::{expand, materialise};

#[derive(Parser)]
#[command(name = "txflow", about = "Declarative transaction-flow experiments")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Expand an experiment spec into its graph IR (with derived relations).
    Expand {
        spec: PathBuf,
        /// Write the IR here instead of stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Build and sign every transaction offline against a chain context.
    Materialise {
        spec: PathBuf,
        #[arg(long)]
        context: PathBuf,
        /// Write the manifest here instead of stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Write the run key (owner of every source and experiment output) as a
    /// cardano-cli signing key, to recover leftover funds with other tools.
    RunKey {
        spec: PathBuf,
        #[arg(long)]
        context: PathBuf,
        /// The `.skey` file to write. Treat it as a secret.
        #[arg(short, long)]
        output: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("txflow: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Expand { spec, output } => {
            let spec = load_spec(&spec)?;
            let ir = expand::expand(&spec)?;
            // serde_yaml ends the document with a newline.
            write(output.as_deref(), |w| Ok(serde_yaml::to_writer(w, &ir)?))
        }
        Command::Materialise {
            spec,
            context,
            output,
        } => {
            let spec = load_spec(&spec)?;
            let ir = expand::expand(&spec)?;
            let context = ChainContext::load(&context)?;
            let manifest = materialise::materialise(&spec.experiment, &ir, spec.seed, &context)?;
            write(output.as_deref(), |w| {
                serde_json::to_writer_pretty(&mut *w, &manifest)?;
                Ok(w.write_all(b"\n")?)
            })
        }
        Command::RunKey {
            spec,
            context,
            output,
        } => {
            let spec = load_spec(&spec)?;
            let ir = expand::expand(&spec)?;
            let context = ChainContext::load(&context)?;
            let ir_hash = txflow::derive::ir_hash(&ir)?;
            let key = materialise::run_key(&context.funding_key, &ir_hash, spec.seed);
            let envelope = serde_json::json!({
                "type": "PaymentSigningKeyShelley_ed25519",
                "description": format!("txflow run key for {}", spec.experiment),
                "cborHex": format!("5820{}", hex::encode(key.0)),
            });
            write_secret(&output, &serde_json::to_string_pretty(&envelope)?)
        }
    }
}

fn load_spec(path: &Path) -> Result<ExperimentSpec> {
    ExperimentSpec::from_yaml(&read_file(path)?).map_err(|e| in_file(path, e))
}

/// Stream a document to a file or stdout. Streamed rather than built as a
/// `String` first: outputs can be hundreds of MB.
fn write(path: Option<&Path>, emit: impl FnOnce(&mut dyn Write) -> Result<()>) -> Result<()> {
    let Some(path) = path else {
        // Not `print!`, so a closed pipe is an error, not a panic.
        let mut out = std::io::BufWriter::new(std::io::stdout().lock());
        emit(&mut out)?;
        return Ok(out.flush()?);
    };
    let file = std::fs::File::create(path).map_err(on_file(path))?;
    let mut out = std::io::BufWriter::new(file);
    emit(&mut out)
        .and_then(|()| Ok(out.flush()?))
        .map_err(|e| in_output(path, e))
}

/// Name the output file an error came from. Serializers wrap I/O failures in
/// their own error types, so every kind is named.
fn in_output(path: &Path, error: Error) -> Error {
    match error {
        Error::Io(e) => on_file(path)(e),
        other => on_file(path)(std::io::Error::other(other.to_string())),
    }
}

/// Write a secret: never over an existing file, readable by the owner only.
fn write_secret(path: &Path, text: &str) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).map_err(on_file(path))?;
    let written = file
        .write_all(text.as_bytes())
        .and_then(|()| file.sync_all());
    if let Err(e) = written {
        // Leave no partial key behind for the next attempt to trip over.
        let _ = std::fs::remove_file(path);
        return Err(on_file(path)(e));
    }
    Ok(())
}
