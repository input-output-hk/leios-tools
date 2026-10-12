//! Snapshot of each pattern's expanded IR (FR-009). Regenerate with
//! `UPDATE_SNAPSHOTS=1 cargo test -p txflow --test snapshots` and review the diff.

use std::path::Path;

use txflow::expand::expand;
use txflow::spec::ExperimentSpec;

const PATTERNS: &[(&str, &str)] = &[
    ("independent", "{ n: 2, outs: 2 }"),
    ("chain", "{ length: 3 }"),
    ("fan_out", "{ width: 3 }"),
    ("fan_in", "{ width: 3 }"),
    ("conflict_set", "{ k: 3 }"),
    ("diamond", "{ lovelace: 3000000 }"),
    ("conflicting_chains", "{ length: 2, conflict: per_step }"),
];

#[test]
fn every_pattern_matches_its_snapshot() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots");
    let update = std::env::var_os("UPDATE_SNAPSHOTS").is_some();
    for (pattern, with) in PATTERNS {
        let spec = ExperimentSpec::from_yaml(&format!(
            "version: 0\nexperiment: snapshot\nseed: 0\nwallets: {{ alice: {{}} }}\n\
             patterns:\n  - {{ id: p, use: {pattern}, with: {with} }}\n"
        ))
        .expect("spec");
        let actual = serde_yaml::to_string(&expand(&spec).expect("expands")).expect("yaml");
        let path = dir.join(format!("{pattern}.yaml"));
        if update {
            std::fs::write(&path, &actual).expect("write snapshot");
            continue;
        }
        let expected = std::fs::read_to_string(&path).expect("snapshot exists");
        assert_eq!(
            actual,
            expected,
            "pattern `{pattern}` differs from {}",
            path.display()
        );
    }
}
