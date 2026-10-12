//! Chain-context loading, including `cardano-cli` file formats.

use txflow::context::{ChainContext, NetworkName};

#[test]
fn reads_cardano_cli_protocol_parameters_and_skey() {
    let dir = std::env::temp_dir().join(format!("txflow-context-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    // Shape of `cardano-cli query protocol-parameters` (abridged) and of a payment `.skey`.
    std::fs::write(
        dir.join("pparams.json"),
        r#"{ "txFeePerByte": 44, "txFeeFixed": 155381, "maxTxSize": 16384,
             "utxoCostPerByte": 4310, "maxBlockBodySize": 90112, "collateralPercentage": 150 }"#,
    )
    .expect("write pparams");
    std::fs::write(
        dir.join("payment.skey"),
        r#"{ "type": "PaymentSigningKeyShelley_ed25519", "description": "Payment Signing Key",
             "cborHex": "582000000000000000000000000000000000000000000000000000000000000000a1" }"#,
    )
    .expect("write skey");
    let ctx = ChainContext::from_yaml(
        "network: testnet\nmagic: 2\nera: conway\npparams_file: pparams.json\n\
         funding:\n  signing_key_file: payment.skey\n  utxos:\n\
         \x20   - { utxo: \"2222222222222222222222222222222222222222222222222222222222222222#3\", lovelace: 5000000 }\n",
        &dir,
    )
    ;
    // Clean up before asserting, so a failure does not leave the key file behind.
    std::fs::remove_dir_all(&dir).ok();
    let ctx = ctx.expect("context loads");

    assert_eq!(ctx.network, NetworkName::Testnet);
    assert_eq!(ctx.pparams.min_fee_a, 44);
    assert_eq!(ctx.pparams.coins_per_utxo_byte, 4310);
    assert_eq!(ctx.funding_key.0[31], 0xa1);
    assert_eq!(ctx.offered[0].index, 3);
}

#[test]
fn rejects_ambiguous_or_malformed_contexts() {
    let base = std::env::temp_dir();
    let utxos = "  utxos: [{ utxo: \"2222222222222222222222222222222222222222222222222222222222222222#0\", lovelace: 1 }]\n";
    let pp = "pparams: { min_fee_a: 1, min_fee_b: 1, max_tx_size: 1, coins_per_utxo_byte: 1 }\n";
    let key =
        "  signing_key_hex: \"00000000000000000000000000000000000000000000000000000000000000a1\"\n";

    let no_key = format!("network: testnet\nmagic: 2\nera: conway\n{pp}funding:\n{utxos}");
    assert!(ChainContext::from_yaml(&no_key, &base).is_err());

    let bad_utxo = format!(
        "network: testnet\nmagic: 2\nera: conway\n{pp}funding:\n{key}  utxos: [{{ utxo: \"abc\", lovelace: 1 }}]\n"
    );
    let err = ChainContext::from_yaml(&bad_utxo, &base)
        .expect_err("bad utxo")
        .to_string();
    assert!(err.contains("`abc`"), "{err}");

    let ok = format!("network: testnet\nmagic: 2\nera: conway\n{pp}funding:\n{key}{utxos}");
    assert!(ChainContext::from_yaml(&ok, &base).is_ok());
    let twice =
        format!("network: testnet\nmagic: 2\nmagic: 3\nera: conway\n{pp}funding:\n{key}{utxos}");
    let err = ChainContext::from_yaml(&twice, &base)
        .expect_err("repeated key")
        .to_string();
    assert!(err.contains("duplicate entry"), "{err}");
}

#[test]
fn duplicate_offers_are_rejected_and_the_key_is_never_printed() {
    let base = std::env::temp_dir();
    let utxo = "{ utxo: \"2222222222222222222222222222222222222222222222222222222222222222#0\", lovelace: 5000000 }";
    let head = "network: testnet\nmagic: 2\nera: conway\npparams: { min_fee_a: 1, min_fee_b: 1, max_tx_size: 1, coins_per_utxo_byte: 1 }\n\
                funding:\n  signing_key_hex: \"00000000000000000000000000000000000000000000000000000000000000a1\"\n";
    let twice = format!("{head}  utxos: [{utxo}, {utxo}]\n");
    let err = ChainContext::from_yaml(&twice, &base)
        .expect_err("duplicate offer")
        .to_string();
    assert!(err.contains("offered twice"), "{err}");

    let once = format!("{head}  utxos: [{utxo}]\n");
    let ctx = ChainContext::from_yaml(&once, &base).expect("loads");
    let printed = format!("{ctx:?}");
    assert!(printed.contains("<redacted>"));
    assert!(!printed.contains("161"), "{printed}"); // 0xa1, the key's last byte
}

#[test]
fn a_verification_key_file_is_not_accepted_as_the_signing_key() {
    let dir = std::env::temp_dir().join(format!("txflow-vkey-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(
        dir.join("payment.vkey"),
        r#"{ "type": "PaymentVerificationKeyShelley_ed25519", "description": "",
             "cborHex": "582000000000000000000000000000000000000000000000000000000000000000a1" }"#,
    )
    .expect("write vkey");
    let result = ChainContext::from_yaml(
        "network: testnet\nmagic: 2\nera: conway\npparams: { min_fee_a: 1, min_fee_b: 1, max_tx_size: 1, coins_per_utxo_byte: 1 }\n\
         funding:\n  signing_key_file: payment.vkey\n\
         \x20 utxos: [{ utxo: \"2222222222222222222222222222222222222222222222222222222222222222#0\", lovelace: 1 }]\n",
        &dir,
    );
    std::fs::remove_dir_all(&dir).ok();
    let err = result.expect_err("vkey rejected").to_string();
    assert!(
        err.contains("PaymentVerificationKeyShelley_ed25519"),
        "{err}"
    );
}

#[test]
fn network_and_magic_must_agree_and_only_payment_keys_are_accepted() {
    let base = std::env::temp_dir();
    let rest = "era: conway\npparams: { min_fee_a: 1, min_fee_b: 1, max_tx_size: 1, coins_per_utxo_byte: 1 }\n\
                funding:\n  signing_key_hex: \"00000000000000000000000000000000000000000000000000000000000000a1\"\n  utxos: []\n";
    let err = ChainContext::from_yaml(&format!("network: mainnet\nmagic: 2\n{rest}"), &base)
        .expect_err("mismatch")
        .to_string();
    assert!(err.contains("does not match magic"), "{err}");
    assert!(ChainContext::from_yaml(
        &format!("network: testnet\nmagic: 764824073\n{rest}"),
        &base
    )
    .is_err());
    assert!(ChainContext::from_yaml(&format!("network: testnet\nmagic: 2\n{rest}"), &base).is_ok());

    let dir = std::env::temp_dir().join(format!("txflow-stake-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(
        dir.join("stake.skey"),
        r#"{ "type": "StakeSigningKeyShelley_ed25519", "description": "",
             "cborHex": "582000000000000000000000000000000000000000000000000000000000000000a1" }"#,
    )
    .expect("write");
    let result = ChainContext::from_yaml(
        "network: testnet\nmagic: 2\nera: conway\npparams: { min_fee_a: 1, min_fee_b: 1, max_tx_size: 1, coins_per_utxo_byte: 1 }\n\
         funding:\n  signing_key_file: stake.skey\n  utxos: []\n",
        &dir,
    );
    std::fs::remove_dir_all(&dir).ok();
    let err = result.expect_err("stake key rejected").to_string();
    assert!(
        err.contains("expected `PaymentSigningKeyShelley_ed25519`"),
        "{err}"
    );
}

#[test]
fn parse_errors_name_the_file() {
    let dir = std::env::temp_dir().join(format!("txflow-parse-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    std::fs::write(dir.join("pp.json"), r#"{ "txFeePerByte": 44 }"#).expect("write");
    let result = ChainContext::from_yaml(
        "network: testnet\nmagic: 2\nera: conway\npparams_file: pp.json\n\
         funding:\n  signing_key_hex: \"00000000000000000000000000000000000000000000000000000000000000a1\"\n  utxos: []\n",
        &dir,
    );
    std::fs::remove_dir_all(&dir).ok();
    let err = result.expect_err("incomplete pparams").to_string();
    assert!(err.contains("pp.json"), "{err}");
}
