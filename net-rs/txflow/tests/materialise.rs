//! Offline materialisation against the demo context.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pallas_traverse::MultiEraTx;
use txflow::context::ChainContext;
use txflow::expand::expand;
use txflow::materialise::{materialise, Manifest, TxKind, METADATA_LABEL};
use txflow::spec::ExperimentSpec;

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name)
}

fn context() -> ChainContext {
    ChainContext::load(&example("demo-context.yaml")).expect("demo context loads")
}

fn build(seed: u64) -> Manifest {
    let text = std::fs::read_to_string(example("conflicting-chains.yaml")).expect("example");
    let spec = ExperimentSpec::from_yaml(&text).expect("spec");
    let ir = expand(&spec).expect("ir");
    materialise(&spec.experiment, &ir, seed, &context()).expect("materialises")
}

fn by_name<'a>(m: &'a Manifest, name: &str) -> &'a txflow::materialise::ManifestTx {
    m.txs
        .iter()
        .find(|t| t.name == name)
        .expect("tx in manifest")
}

#[test]
fn materialise_is_deterministic_and_seeded() {
    let a = build(42);
    assert_eq!(a, build(42));
    let other = build(43);
    assert_ne!(a.run_id, other.run_id);
    assert_ne!(
        by_name(&a, "chains/a1").tx_id,
        by_name(&other, "chains/a1").tx_id
    );
}

#[test]
fn manifest_lists_funding_then_experiment_in_topological_order() {
    let m = build(42);
    let order: Vec<&str> = m.txs.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        order,
        [
            "fund/0",
            "chains/a1",
            "chains/b1",
            "chains/a2",
            "chains/b2",
            "chains/a3",
            "chains/b3"
        ]
    );
    assert_eq!(m.txs[0].kind, TxKind::Funding);
    assert!(m.txs[1..].iter().all(|t| t.kind == TxKind::Experiment));
    for (i, tx) in m.txs.iter().enumerate() {
        assert_eq!(tx.index, i);
    }
    assert_eq!(by_name(&m, "chains/b2").conflict_sets, ["chains/c2"]);
    assert_eq!(by_name(&m, "chains/b2").orphan_on, ["chains/c1"]);
}

#[test]
fn built_txs_decode_and_link_as_the_graph_says() {
    let m = build(42);
    let ctx = context();
    for entry in &m.txs {
        let bytes = hex::decode(&entry.cbor_hex).expect("hex");
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        assert_eq!(hex::encode(tx.hash()), entry.tx_id, "{}", entry.name);

        // Fee covers the linear fee of the signed size.
        let fee = tx.fee().expect("fee");
        assert_eq!(fee, entry.fee);
        let min_fee = ctx.pparams.min_fee_a * bytes.len() as u64 + ctx.pparams.min_fee_b;
        assert!(fee >= min_fee, "{}: fee {fee} < {min_fee}", entry.name);
        assert!(fee < min_fee + 1_000, "{}: fee overpays", entry.name);

        // The wire agrees with the manifest.
        let outputs: Vec<u64> = tx.outputs().iter().map(|o| o.value().coin()).collect();
        assert_eq!(outputs, entry.outputs);

        // Every output is above min-UTxO computed from its real size.
        for output in tx.outputs() {
            let min = (160 + output.encode().len() as u64) * ctx.pparams.coins_per_utxo_byte;
            assert!(output.value().coin() >= min, "{}", entry.name);
        }

        // Label 674 carries the IR hash prefix and the run id with the index.
        let meta = format!(
            "{:?}",
            tx.metadata().find(METADATA_LABEL).expect("label 674")
        );
        let ir_prefix = &m.ir_hash["blake2b256:".len().."blake2b256:".len() + 16];
        assert!(meta.contains(&format!("txflow:{ir_prefix}")), "{meta}");
        assert!(
            meta.contains(&format!("{}#{}", m.run_id, entry.index)),
            "{meta}"
        );

        // Exactly one signature.
        assert_eq!(tx.vkey_witnesses().len(), 1);
    }

    // a2 spends a1's change (output 0) and the shared source c2 from funding.
    let fund = by_name(&m, "fund/0");
    let a1 = by_name(&m, "chains/a1");
    let a2_bytes = hex::decode(&by_name(&m, "chains/a2").cbor_hex).expect("hex");
    let a2 = MultiEraTx::decode(&a2_bytes).expect("decodes");
    let mut spent: Vec<(String, u64)> = a2
        .inputs()
        .iter()
        .map(|i| (hex::encode(i.hash()), i.index()))
        .collect();
    spent.sort();
    // Funding outputs follow source name order: c1, c2, c3, wa, wb, change.
    let mut expected = vec![(a1.tx_id.clone(), 0), (fund.tx_id.clone(), 1)];
    expected.sort();
    assert_eq!(spent, expected);

    // The manifest's resolved inputs agree with the wire.
    let a2_entry = by_name(&m, "chains/a2");
    let mut listed: Vec<String> = a2_entry.inputs.iter().map(|i| i.utxo.clone()).collect();
    listed.sort();
    let mut on_wire: Vec<String> = spent.iter().map(|(h, i)| format!("{h}#{i}")).collect();
    on_wire.sort();
    assert_eq!(listed, on_wire);
    assert_eq!(a2_entry.inputs[0].reference, "chains/a1.0");

    // a1 and b1 both spend c1: the conflict is real on the wire.
    let c1 = (fund.tx_id.clone(), 0u64);
    for name in ["chains/a1", "chains/b1"] {
        let bytes = hex::decode(&by_name(&m, name).cbor_hex).expect("hex");
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        assert!(tx
            .inputs()
            .iter()
            .any(|i| (hex::encode(i.hash()), i.index()) == c1));
    }
}

#[test]
fn every_tx_conserves_value() {
    let m = build(42);
    // Value of every UTxO that exists before or during the run.
    let mut value: BTreeMap<String, u64> = context()
        .offered
        .iter()
        .map(|u| (format!("{}#{}", hex::encode(u.tx_id), u.index), u.lovelace))
        .collect();
    for entry in &m.txs {
        let bytes = hex::decode(&entry.cbor_hex).expect("hex");
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        let mut spent = 0u64;
        for input in tx.inputs() {
            let key = format!("{}#{}", hex::encode(input.hash()), input.index());
            spent += value.get(&key).copied().expect("input exists");
        }
        let produced: u64 = tx.outputs().iter().map(|o| o.value().coin()).sum();
        assert_eq!(spent, produced + entry.fee, "{}", entry.name);
        for (i, output) in tx.outputs().iter().enumerate() {
            value.insert(format!("{}#{i}", entry.tx_id), output.value().coin());
        }
    }
}

#[test]
fn run_key_owns_the_run_address() {
    use pallas_addresses::{
        Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
    };
    use pallas_crypto::hash::Hasher;
    use pallas_crypto::key::ed25519::SecretKey;

    let m = build(42);
    let key = txflow::materialise::run_key(&context().funding_key, &m.ir_hash, 42);
    let public = SecretKey::from(key.0).public_key();
    let address = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::key_hash(Hasher::<224>::hash(public.as_ref())),
        ShelleyDelegationPart::Null,
    ));
    assert_eq!(address.to_bech32().expect("bech32"), m.run_address);
}

#[test]
fn funding_conserves_value_and_returns_change() {
    let m = build(42);
    let fund = by_name(&m, "fund/0");
    let offered = context().offered[0].lovelace;
    let total: u64 = fund.outputs.iter().sum();
    assert_eq!(total + fund.fee, offered);
    assert_eq!(
        &fund.outputs[..5],
        &[2_000_000, 2_000_000, 2_000_000, 10_000_000, 10_000_000]
    );
}

#[test]
fn too_little_funding_is_reported_before_anything_is_built() {
    let text = std::fs::read_to_string(example("conflicting-chains.yaml")).expect("example");
    let spec = ExperimentSpec::from_yaml(&text).expect("spec");
    let ir = expand(&spec).expect("ir");
    let mut ctx = context();
    ctx.offered[0].lovelace = 20_000_000;
    let err = materialise(&spec.experiment, &ir, 42, &ctx)
        .expect_err("must fail")
        .to_string();
    assert!(err.contains("offered UTxOs hold 20000000"), "{err}");
}

#[test]
fn many_sources_are_funded_by_chained_funding_txs() {
    let spec = ExperimentSpec::from_yaml(
        "version: 0\nexperiment: wide\nseed: 7\nwallets: { alice: {} }\n\
         patterns:\n  - { id: p, use: independent, with: { n: 250, lovelace: 2000000 } }\n",
    )
    .expect("spec");
    let ir = expand(&spec).expect("ir");
    let m = materialise(&spec.experiment, &ir, spec.seed, &context()).expect("materialises");
    let funding: Vec<_> = m.txs.iter().filter(|t| t.kind == TxKind::Funding).collect();
    assert_eq!(funding.len(), 2);
    assert_eq!(funding[0].outputs.len(), 201);
    assert_eq!(funding[1].outputs.len(), 51);
    // The second funding tx spends the first one's change.
    assert_eq!(funding[1].inputs[0].reference, "fund/0.200");
    assert_eq!(
        funding[1].inputs[0].utxo,
        format!("{}#200", funding[0].tx_id)
    );
    let bytes = hex::decode(&funding[1].cbor_hex).expect("hex");
    let tx = MultiEraTx::decode(&bytes).expect("decodes");
    let input = &tx.inputs()[0];
    assert_eq!(hex::encode(input.hash()), funding[0].tx_id);
    assert_eq!(input.index(), 200);
    assert_eq!(m.txs.len(), 252);
}

fn materialise_yaml(yaml: &str, ctx: &ChainContext) -> txflow::error::Result<Manifest> {
    let spec = ExperimentSpec::from_yaml(yaml).expect("spec");
    let ir = expand(&spec).expect("ir");
    materialise(&spec.experiment, &ir, spec.seed, ctx)
}

fn offered(n: u64, lovelace: u64) -> Vec<txflow::context::OfferedUtxo> {
    (0..n)
        .map(|i| txflow::context::OfferedUtxo {
            tx_id: [0x22; 32],
            index: i,
            lovelace,
        })
        .collect()
}

#[test]
fn reference_input_order_does_not_change_tx_ids() {
    let spec = |refs: &str| {
        format!(
            "version: 0\nexperiment: r\nseed: 1\nwallets: {{ alice: {{}} }}\n\
             sources: {{ f: {{ owner: alice, lovelace: 5000000 }}, r1: {{ owner: alice, lovelace: 2000000 }}, r2: {{ owner: alice, lovelace: 2000000 }} }}\n\
             txs:\n  q: {{ inputs: [f], reference_inputs: [{refs}], outputs: [{{ to: alice, lovelace: change }}] }}\n"
        )
    };
    let a = materialise_yaml(&spec("r1, r2"), &context()).expect("a");
    let b = materialise_yaml(&spec("r2, r1"), &context()).expect("b");
    assert_eq!(a, b);
    // Expand already sorts; reverse the IR's list so the builder's own sort is tested.
    let parsed = ExperimentSpec::from_yaml(&spec("r1, r2")).expect("spec");
    let mut ir = expand(&parsed).expect("ir");
    ir.txs.get_mut("q").expect("q").reference_inputs.reverse();
    let c = materialise(&parsed.experiment, &ir, parsed.seed, &context()).expect("c");
    assert_eq!(
        a.txs.last().map(|t| &t.tx_id),
        c.txs.last().map(|t| &t.tx_id)
    );
}

#[test]
fn wide_independent_txs_are_funded_by_default() {
    let spec = "version: 0\nexperiment: w\nseed: 1\nwallets: { alice: {} }\n\
                patterns:\n  - { id: p, use: independent, with: { outs: 12 } }\n";
    materialise_yaml(spec, &context()).expect("12 outputs");
}

#[test]
fn long_chains_are_funded_by_default() {
    let spec = "version: 0\nexperiment: l\nseed: 1\nwallets: { alice: {} }\n\
                patterns:\n  - { id: c, use: chain, with: { length: 80, outs: 2 } }\n  \
                - { id: r, use: conflicting_chains, with: { length: 80, conflict: root } }\n";
    let mut ctx = context();
    ctx.offered = offered(1, 1_000_000_000);
    let m = materialise_yaml(spec, &ctx).expect("long chains");
    assert!(m.txs.iter().any(|t| t.name == "c/t80"));
    assert!(m.txs.iter().any(|t| t.name == "r/b80"));
}

#[test]
fn funding_splits_by_size_for_many_inputs_or_a_small_max_tx_size() {
    let wide = "version: 0\nexperiment: w\nseed: 1\nwallets: { alice: {} }\n\
                patterns:\n  - { id: p, use: independent, with: { n: 200, lovelace: 1500000 } }\n";
    // Hundreds of small offered UTxOs: the first funding tx has room for fewer sources.
    let mut ctx = context();
    ctx.offered = offered(400, 1_000_000);
    let m = materialise_yaml(wide, &ctx).expect("many inputs");
    let funding: Vec<_> = m.txs.iter().filter(|t| t.kind == TxKind::Funding).collect();
    assert!(funding.len() >= 2);
    assert!(funding
        .iter()
        .all(|t| t.size as u64 <= ctx.pparams.max_tx_size));

    // A small max_tx_size: more, smaller funding txs.
    let mut ctx = context();
    ctx.pparams.max_tx_size = 4096;
    let m = materialise_yaml(wide, &ctx).expect("small max size");
    let funding: Vec<_> = m.txs.iter().filter(|t| t.kind == TxKind::Funding).collect();
    assert!(funding.len() >= 4, "{}", funding.len());
    assert!(funding.iter().all(|t| t.size <= 4096));
}

#[test]
fn an_experiment_without_sources_spends_nothing() {
    let m = materialise_yaml(
        "version: 0\nexperiment: empty\nseed: 1\nwallets: { alice: {} }\n",
        &context(),
    )
    .expect("materialises");
    assert!(m.funding_utxos.is_empty());
    assert!(m.txs.is_empty());
}

#[test]
fn errors_name_the_transaction_once() {
    let err = materialise_yaml(
        "version: 0\nexperiment: e\nseed: 1\nwallets: { alice: {} }\n\
         sources: { s: { owner: alice, lovelace: 2000000 } }\n\
         txs:\n  t: { inputs: [s], outputs: [{ to: alice, lovelace: 3000000 }, { to: alice, lovelace: change }] }\n",
        &context(),
    )
    .expect_err("underfunded")
    .to_string();
    assert!(err.starts_with("materialise: tx `t`: inputs hold"), "{err}");
}

fn base_address(network: pallas_addresses::Network) -> String {
    use pallas_addresses::{Address, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
    use pallas_crypto::hash::Hash;
    Address::Shelley(ShelleyAddress::new(
        network,
        ShelleyPaymentPart::key_hash(Hash::new([7; 28])),
        ShelleyDelegationPart::key_hash(Hash::new([9; 28])),
    ))
    .to_bech32()
    .expect("bech32")
}

#[test]
fn only_the_last_funding_tx_pays_change_address() {
    let wide = "version: 0\nexperiment: w\nseed: 1\nwallets: { alice: {} }\n\
                patterns:\n  - { id: p, use: independent, with: { n: 250, lovelace: 2000000 } }\n";
    let mut ctx = context();
    let change = base_address(pallas_addresses::Network::Testnet);
    ctx.change_address = Some(change.clone());
    let m = materialise_yaml(wide, &ctx).expect("materialises");
    assert_eq!(m.change_address.as_deref(), Some(change.as_str()));
    let funding: Vec<_> = m.txs.iter().filter(|t| t.kind == TxKind::Funding).collect();
    assert_eq!(funding.len(), 2);
    let change_of = |entry: &txflow::materialise::ManifestTx| {
        let bytes = hex::decode(&entry.cbor_hex).expect("hex");
        let tx = MultiEraTx::decode(&bytes).expect("decodes");
        let outputs = tx.outputs();
        let last = outputs.last().expect("change output");
        last.address()
            .expect("address")
            .to_bech32()
            .expect("bech32")
    };
    // fund/1 spends fund/0's change with the funding key, so it must stay there.
    assert_eq!(change_of(funding[0]), m.funding_address);
    assert_eq!(change_of(funding[1]), change);
}

#[test]
fn change_address_must_be_a_payment_address_on_the_network() {
    let spec = std::fs::read_to_string(example("conflicting-chains.yaml")).expect("example");
    let mut ctx = context();
    ctx.change_address = Some(base_address(pallas_addresses::Network::Mainnet));
    let err = materialise_yaml(&spec, &ctx)
        .expect_err("mainnet")
        .to_string();
    assert!(err.contains("another network"), "{err}");
}

#[test]
fn manifest_does_not_depend_on_offer_order() {
    let spec = std::fs::read_to_string(example("conflicting-chains.yaml")).expect("example");
    let mut ctx = context();
    ctx.offered = offered(2, 20_000_000);
    let a = materialise_yaml(&spec, &ctx).expect("a");
    ctx.offered.reverse();
    let b = materialise_yaml(&spec, &ctx).expect("b");
    assert_eq!(a.funding_utxos.len(), 2);
    assert_eq!(a, b);
    assert!(a.pparams_hash.starts_with("blake2b256:"));
    assert_eq!(a.network_magic, 164);
}

#[test]
fn funding_txs_name_the_source_each_output_holds() {
    let m = build(42);
    assert_eq!(
        m.txs[0].sources,
        [
            "chains/c1",
            "chains/c2",
            "chains/c3",
            "chains/wa",
            "chains/wb"
        ]
    );
    assert_eq!(m.txs[0].outputs.len(), 6);
}

#[test]
fn change_address_must_not_be_a_script() {
    use pallas_addresses::{
        Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
    };
    use pallas_crypto::hash::Hash;
    let script = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::script_hash(Hash::new([3; 28])),
        ShelleyDelegationPart::Null,
    ))
    .to_bech32()
    .expect("bech32");
    let spec = std::fs::read_to_string(example("conflicting-chains.yaml")).expect("example");
    let mut ctx = context();
    ctx.change_address = Some(script);
    let err = materialise_yaml(&spec, &ctx)
        .expect_err("script")
        .to_string();
    assert!(err.contains("script address"), "{err}");
}
