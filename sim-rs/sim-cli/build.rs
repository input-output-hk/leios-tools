use std::{error::Error, process::Command};

use vergen_gitcl::{Emitter, GitclBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    // Vergen 1.x resolves branch refs under the worktree git directory, where
    // common refs do not live. Ask git to resolve them so commits rebuild the
    // version even when only documentation changed in a linked worktree.
    let branch = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()?;
    let branch = String::from_utf8(branch.stdout)?;
    for name in ["HEAD", "packed-refs", branch.trim()] {
        if name.is_empty() {
            continue;
        }
        let path = Command::new("git")
            .args(["rev-parse", "--git-path", name])
            .output()?;
        if path.status.success() {
            println!(
                "cargo:rerun-if-changed={}",
                String::from_utf8(path.stdout)?.trim()
            );
        }
    }
    let git = GitclBuilder::default().sha(true).build()?;
    Emitter::default().add_instructions(&git)?.emit()?;
    Ok(())
}
