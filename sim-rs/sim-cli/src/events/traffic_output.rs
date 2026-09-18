//! Reserve a temporary report before starting the simulation. Publish it only
//! after both the simulation and monitor finish successfully.
use std::{
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};

pub(crate) struct TrafficOutput {
    destination: PathBuf,
    temporary: PathBuf,
    file: fs::File,
}

fn absolute_output(path: &Path) -> Result<PathBuf> {
    let mut path = path.to_path_buf();
    for _ in 0..40 {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let absolute = parent
            .canonicalize()?
            .join(path.file_name().context("output needs a file name")?);
        match fs::read_link(&absolute) {
            Ok(target) => {
                path = if target.is_absolute() {
                    target
                } else {
                    absolute.parent().unwrap().join(target)
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                ) =>
            {
                return Ok(absolute);
            }
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("too many symbolic links in output path")
}

impl TrafficOutput {
    pub fn reserve(path: &Path, event_path: Option<&Path>) -> Result<Self> {
        if fs::symlink_metadata(path).is_ok() {
            anyhow::bail!("traffic output already exists: {}", path.display());
        }
        let destination = absolute_output(path)?;
        // symlink_metadata also rejects a dangling symlink at the destination.
        match fs::symlink_metadata(&destination) {
            Ok(_) => anyhow::bail!("traffic output already exists: {}", path.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(event_path) = event_path {
            ensure!(
                destination != absolute_output(event_path)?,
                "traffic and event outputs must be different files"
            );
        }
        let temporary = destination.with_file_name(format!(
            ".vote-traffic-{}-{:016x}.tmp",
            std::process::id(),
            rand::random::<u64>()
        ));
        let file = fs::File::create_new(&temporary)?;
        Ok(Self {
            destination,
            temporary,
            file,
        })
    }

    pub fn write(&mut self, report: &impl serde::Serialize) -> Result<()> {
        let mut writer = BufWriter::new(&mut self.file);
        serde_json::to_writer(&mut writer, report)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    pub fn publish(self) -> Result<()> {
        // Same-directory hard linking publishes a complete file atomically and
        // fails if another process created the destination in the meantime.
        fs::hard_link(&self.temporary, &self.destination)?;
        Ok(())
    }
}

impl Drop for TrafficOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temporary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_fail_before_writing_and_failed_captures_can_retry() {
        let root =
            std::env::temp_dir().join(format!("vote-output-test-{:016x}", rand::random::<u64>()));
        fs::create_dir(&root).unwrap();
        let path = root.join("report.json");
        assert!(TrafficOutput::reserve(&path, Some(&root.join("./report.json"))).is_err());
        assert!(!path.exists());
        drop(TrafficOutput::reserve(&path, None).unwrap());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        let mut output = TrafficOutput::reserve(&path, None).unwrap();
        output.write(&vec![94]).unwrap();
        assert!(!path.exists());
        output.publish().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "[94]\n");
        assert!(TrafficOutput::reserve(&path, None).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "[94]\n");
        fs::remove_dir_all(root).unwrap();
    }
}
