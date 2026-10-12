//! Error type shared by every stage.

/// Everything that can go wrong while reading, expanding or materialising an experiment.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The experiment spec is malformed or uses something unsupported.
    #[error("spec: {0}")]
    Spec(String),
    /// The graph IR is inconsistent (dangling reference, cycle, ...).
    #[error("graph: {0}")]
    Graph(String),
    /// The chain-context file is malformed.
    #[error("context: {0}")]
    Context(String),
    /// A transaction could not be built (insufficient value, too large, ...).
    #[error("materialise: {0}")]
    Materialise(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A file could not be parsed.
    #[error("{path}: {message}")]
    Parse { path: String, message: String },
    /// An I/O error on a named file.
    #[error("{path}: {source}")]
    File {
        path: String,
        source: std::io::Error,
    },
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Read a whole file, naming it in the error.
pub fn read_file(path: &std::path::Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|source| Error::File {
        path: path.display().to_string(),
        source,
    })
}

/// Name the file a YAML or JSON parse error came from.
pub fn in_file(path: &std::path::Path, error: Error) -> Error {
    match error {
        Error::Yaml(e) => Error::Parse {
            path: path.display().to_string(),
            message: e.to_string(),
        },
        Error::Json(e) => Error::Parse {
            path: path.display().to_string(),
            message: e.to_string(),
        },
        other => other,
    }
}

/// Attach `path` to an I/O error.
pub fn on_file(path: &std::path::Path) -> impl FnOnce(std::io::Error) -> Error + '_ {
    move |source| Error::File {
        path: path.display().to_string(),
        source,
    }
}
