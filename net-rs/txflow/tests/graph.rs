//! Expansion and derived relations.

use std::path::Path;

use txflow::expand::expand;
use txflow::ir::{Ir, Ref};
use txflow::spec::ExperimentSpec;

fn spec(yaml: &str) -> ExperimentSpec {
    ExperimentSpec::from_yaml(yaml).expect("spec parses")
}

fn expand_yaml(yaml: &str) -> Ir {
    expand(&spec(yaml)).expect("spec expands")
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// `count: 1` is left out, so names have no `_<k>` suffix.
fn chains(conflict: &str, count: u32) -> String {
    let count = if count == 1 {
        String::new()
    } else {
        format!("count: {count}, ")
    };
    format!(
        "version: 0\nexperiment: t\nseed: 1\nwallets: {{ alice: {{}} }}\n\
         patterns:\n  - {{ id: chains, use: conflicting_chains, {count}\
         with: {{ length: 3, conflict: {conflict} }} }}\n"
    )
}

#[test]
fn worked_example_expands_to_six_txs_and_three_conflict_sets() {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/conflicting-chains.yaml"),
    )
    .expect("example exists");
    let ir = expand_yaml(&text);
    let derived = ir.derived.as_ref().expect("derived");

    assert_eq!(ir.txs.len(), 6);
    assert_eq!(
        ir.sources.keys().cloned().collect::<Vec<_>>(),
        names(&[
            "chains/c1",
            "chains/c2",
            "chains/c3",
            "chains/wa",
            "chains/wb"
        ])
    );
    assert_eq!(
        derived.topo_order,
        names(&[
            "chains/a1",
            "chains/b1",
            "chains/a2",
            "chains/b2",
            "chains/a3",
            "chains/b3"
        ])
    );
    for i in 1..=3 {
        assert_eq!(
            derived.conflict_sets.get(&format!("chains/c{i}")),
            Some(&names(&[&format!("chains/a{i}"), &format!("chains/b{i}")]))
        );
    }
    assert_eq!(derived.conflict_sets.len(), 3);
    assert!(!derived.orphan_on.contains_key("chains/a1"));
    assert_eq!(
        derived.orphan_on.get("chains/b2"),
        Some(&names(&["chains/c1"]))
    );
    assert_eq!(
        derived.orphan_on.get("chains/a3"),
        Some(&names(&["chains/c1", "chains/c2"]))
    );
    assert!(derived.reference_contention.is_empty());
    assert_eq!(ir.txs["chains/a1"].labels, names(&["side-a"]));
}

#[test]
fn root_conflict_leaves_later_txs_orphaned_not_conflicting() {
    let ir = expand_yaml(&chains("root", 1));
    let derived = ir.derived.as_ref().expect("derived");
    assert_eq!(derived.conflict_sets.len(), 1);
    assert!(derived.conflict_sets.contains_key("chains/c1"));
    assert!(!ir.sources.contains_key("chains/c2"));
    assert_eq!(
        derived.orphan_on.get("chains/b3"),
        Some(&names(&["chains/c1"]))
    );
}

#[test]
fn count_namespaces_instances_independently() {
    let ir = expand_yaml(&chains("per_step", 2));
    assert_eq!(ir.txs.len(), 12);
    assert!(ir.txs.contains_key("chains_0/a1"));
    assert!(ir.txs.contains_key("chains_1/b3"));
    let derived = ir.derived.as_ref().expect("derived");
    // Instances never share an output, so conflicts stay within an instance.
    assert_eq!(derived.conflict_sets.len(), 6);
    assert_eq!(
        derived.conflict_sets.get("chains_1/c2"),
        Some(&names(&["chains_1/a2", "chains_1/b2"]))
    );
}

#[test]
fn params_and_expressions_scale_patterns() {
    let ir = expand_yaml(
        "version: 0\nexperiment: t\nseed: 1\nparams: { scale: 10 }\nwallets: { alice: {} }\n\
         patterns:\n  - { id: c, use: chain, with: { length: \"{{ scale * 2 }}\" } }\n",
    );
    assert_eq!(ir.txs.len(), 20);
    assert_eq!(ir.txs["c/t20"].inputs, names(&["c/t19.0"]));
}

#[test]
fn reference_inputs_give_contention_not_conflicts() {
    let ir = expand_yaml(
        "version: 0\nexperiment: t\nseed: 1\nwallets: { alice: {} }\n\
         sources:\n  pool: { owner: alice, lovelace: 5000000 }\n  f1: { owner: alice, lovelace: 5000000 }\n  f2: { owner: alice, lovelace: 5000000 }\n\
         txs:\n\
         \x20 batch: { inputs: [pool], outputs: [{ to: alice, lovelace: change }] }\n\
         \x20 quote1: { inputs: [f1], reference_inputs: [pool], outputs: [{ to: alice, lovelace: change }] }\n\
         \x20 quote2: { inputs: [f2], reference_inputs: [pool], outputs: [{ to: alice, lovelace: change }] }\n",
    );
    let derived = ir.derived.as_ref().expect("derived");
    assert!(derived.conflict_sets.is_empty());
    let contention = derived
        .reference_contention
        .get("pool")
        .expect("contention");
    assert_eq!(contention.spent_by, names(&["batch"]));
    assert_eq!(contention.read_by, names(&["quote1", "quote2"]));
}

#[test]
fn ir_hash_is_stable_and_content_addressed() {
    let a = expand_yaml(&chains("per_step", 1));
    let b = expand_yaml(&chains("per_step", 1));
    let c = expand_yaml(&chains("root", 1));
    let hash = |ir: &Ir| ir.derived.as_ref().expect("derived").hash.clone();
    assert_eq!(hash(&a), hash(&b));
    assert_ne!(hash(&a), hash(&c));
    assert!(hash(&a).starts_with("blake2b256:"));
}

#[test]
fn ref_parse_distinguishes_sources_and_outputs() {
    assert_eq!(Ref::parse("x/s").ok(), Some(Ref::Source("x/s".to_string())));
    assert_eq!(
        Ref::parse("x/t1.2").ok(),
        Some(Ref::Output {
            tx: "x/t1".to_string(),
            index: 2
        })
    );
}

fn expect_error(yaml: &str, needle: &str) {
    let err = expand(&spec(yaml))
        .expect_err("expansion must fail")
        .to_string();
    assert!(err.contains(needle), "`{err}` does not mention `{needle}`");
}

const HEAD: &str = "version: 0\nexperiment: t\nseed: 1\nwallets: { alice: {} }\n";

#[test]
fn invalid_graphs_are_rejected_with_the_tx_named() {
    expect_error(
        &format!(
            "{HEAD}txs:\n  t: {{ inputs: [nope], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "`nope` is neither a source",
    );
    expect_error(
        &format!(
            "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}, {{ to: alice, lovelace: change }}] }}\n"
        ),
        "exactly one `change`",
    );
    expect_error(
        &format!(
            "{HEAD}txs:\n  x: {{ inputs: [y.0], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
             y: {{ inputs: [x.0], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "cycle",
    );
    expect_error(
        &format!(
            "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
             u: {{ inputs: [t.1], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "has no output 1",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: c, use: chain, with: {{ lenght: 3 }} }}\n"),
        "no argument `lenght`",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: c, use: chain, with: {{ wallet: bob }} }}\n"),
        "unknown wallet `bob`",
    );
}

#[test]
fn review_cases_are_rejected() {
    // One spelling per output: `t.00` would dodge conflict detection.
    expect_error(
        &format!(
            "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
             u: {{ inputs: [t.00], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "leading zero",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: p, use: chain }}\n  - {{ id: p, use: fan_in }}\n"),
        "pattern id `p` is used twice",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: p, use: no_such, count: 0 }}\n"),
        "count must be at least 1",
    );
    expect_error(
        "version: 0\nexperiment: t\nseed: 1\nwallets: { zed: {}, alice: {} }\n\
         patterns:\n  - { id: p, use: chain }\n",
        "`wallet` is required",
    );
}

#[test]
fn ir_hash_sorts_inputs_itself() {
    // Expand already sorts; reverse the lists so the hash's own sort is what is tested.
    let ir = expand_yaml(&chains("per_step", 1));
    let mut shuffled = ir.clone();
    for tx in shuffled.txs.values_mut() {
        tx.inputs.reverse();
    }
    assert_ne!(shuffled.txs["chains/a1"].inputs, ir.txs["chains/a1"].inputs);
    assert_eq!(
        txflow::derive::ir_hash(&shuffled).expect("hash"),
        txflow::derive::ir_hash(&ir).expect("hash")
    );
}

#[test]
fn a_repeated_key_is_an_error_not_a_silent_overwrite() {
    let yaml = format!(
        "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }} }}\n\
         txs:\n  t: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
         t: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n"
    );
    let err = ExperimentSpec::from_yaml(&yaml)
        .expect_err("duplicate tx")
        .to_string();
    assert!(err.contains("duplicate entry with key \"t\""), "{err}");
}

#[test]
fn wallet_names_follow_the_naming_rule() {
    expect_error(
        "version: 0\nexperiment: t\nseed: 1\nwallets: { \"Bad Wallet\": {} }\n",
        "name `Bad Wallet` must match",
    );
}

#[test]
fn input_order_does_not_change_the_ir_hash() {
    let tx = |inputs: &str| {
        expand_yaml(&format!(
            "{HEAD}sources: {{ a: {{ owner: alice, lovelace: 5000000 }}, b: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t: {{ inputs: [{inputs}], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ))
        .derived
        .expect("derived")
        .hash
    };
    assert_eq!(tx("a, b"), tx("b, a"));
}

#[test]
fn written_count_names_copies_whatever_its_value() {
    let ir = expand_yaml(&format!(
        "{HEAD}params: {{ copies: 1 }}\npatterns:\n  - {{ id: c, use: chain, count: \"{{{{ copies }}}}\" }}\n"
    ));
    assert!(ir.txs.contains_key("c_0/t1"));
}

#[test]
fn generate_errors_name_the_instance() {
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: right, use: chain, with: {{ length: 0 }} }}\n"),
        "pattern `right`: `length` must be at least 1",
    );
}

#[test]
fn instances_cannot_share_a_namespace() {
    expect_error(
        &format!(
            "{HEAD}patterns:\n  - {{ id: x, use: chain, count: 2 }}\n  - {{ id: x_0, use: fan_out }}\n"
        ),
        "namespace `x_0/` is already used",
    );
}

#[test]
fn expanded_inputs_are_listed_sorted() {
    let ir = expand_yaml(&chains("per_step", 1));
    assert_eq!(
        ir.txs["chains/a1"].inputs,
        names(&["chains/c1", "chains/wa"])
    );
}

#[test]
fn spending_what_an_ancestor_already_spent_is_rejected() {
    expect_error(
        &format!(
            "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }}, f: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t1: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
             t2: {{ inputs: [t1.0, s], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "its ancestor `t1` already spends",
    );
    expect_error(
        &format!(
            "{HEAD}sources: {{ s: {{ owner: alice, lovelace: 5000000 }}, f: {{ owner: alice, lovelace: 5000000 }} }}\n\
             txs:\n  t1: {{ inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n  \
             t2: {{ inputs: [t1.0], reference_inputs: [s], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        ),
        "its ancestor `t1` already spends",
    );
}

#[test]
fn depending_on_both_sides_of_a_conflict_is_rejected() {
    let both = |extra: &str| {
        format!(
            "{HEAD}patterns:\n  - {{ id: c, use: conflict_set, with: {{ k: 2 }} }}\n\
             txs:\n  d: {{ {extra}, outputs: [{{ to: alice, lovelace: change }}] }}\n"
        )
    };
    expect_error(&both("inputs: [c/x1.0, c/x2.0]"), "depends on both");
    expect_error(
        &both("inputs: [c/x1.0], reference_inputs: [c/x2.0]"),
        "depends on both",
    );
}

#[test]
fn large_conflict_graphs_expand() {
    // Was quadratic per pair of conflicting txs; now one pass.
    let ir = expand_yaml(&format!(
        "{HEAD}patterns:\n  - {{ id: c, use: conflict_set, with: {{ k: 3000 }} }}\n  \
         - {{ id: l, use: conflicting_chains, with: {{ length: 1000 }} }}\n"
    ));
    assert_eq!(ir.txs.len(), 5000);
}

#[test]
fn sizes_are_bounded_and_errors_name_the_argument() {
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: p, use: independent, with: {{ n: 1000000000 }} }}\n"),
        "`n` is 1000000000, the maximum is 100000",
    );
    expect_error(
        &format!(
            "{HEAD}patterns:\n  - {{ id: p, use: chain, count: \"{{{{ 10 * 1000000 }}}}\" }}\n"
        ),
        "count is 10000000",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: p, use: chain, with: {{ length: \"{{{{ lenght }}}}\" }} }}\n"),
        "pattern `p`, `length`: unknown parameter `lenght`",
    );
}

#[test]
fn diamond_rejoins_its_branches() {
    let ir = expand_yaml(&format!("{HEAD}patterns:\n  - {{ id: d, use: diamond }}\n"));
    assert_eq!(ir.txs["d/merge"].inputs, names(&["d/left.0", "d/right.0"]));
    let derived = ir.derived.expect("derived");
    assert!(derived.conflict_sets.is_empty());
    assert_eq!(
        derived.topo_order.first().map(String::as_str),
        Some("d/root")
    );
}

#[test]
fn ancestor_relations_are_bounded() {
    expect_error(
        &format!(
            "{HEAD}patterns:\n  - {{ id: l, use: conflicting_chains, with: {{ length: 5000 }} }}\n"
        ),
        "ancestor-conflict relations",
    );
}

#[test]
fn per_tx_sizes_are_bounded() {
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: f, use: fan_out, with: {{ width: 100000 }} }}\n"),
        "`width` is 100000, the maximum is 1000",
    );
    expect_error(
        &format!("{HEAD}patterns:\n  - {{ id: p, use: independent, with: {{ n: 100000, outs: 1000 }} }}\n"),
        "graph elements",
    );
}
