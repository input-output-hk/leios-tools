use std::net::ToSocketAddrs;

use net_core::bearer::tcp::TcpBearer;
use net_core::mux::scheduler::RoundRobin;
use net_core::mux::scheduler::TrafficClass;
use net_core::mux::{CodecRecv, CodecSend, Mux, MuxConfig, ProtocolConfig, MODE_INITIATOR};
use net_core::protocols::handshake;
use net_core::protocols::handshake::n2n;

/// Options controlling how the handshake is performed.
pub struct Options {
    pub magic: u64,
    /// Send a query handshake (ask the peer for its full version table).
    pub query: bool,
    /// Advertise initiator-only diffusion (default duplex).
    pub initiator_only: bool,
    /// PeerSharing flag to advertise (0 or 1).
    pub peer_sharing: u8,
    /// Versions to propose (empty = all supported: 14, 15, 16).
    pub propose: Vec<u64>,
    /// Raw per-version params overrides, each as "N=HEX".
    pub raw_version: Vec<String>,
}

/// Format a decoded version-data record for display.
fn describe(params: &[u8]) -> String {
    let hex: String = params.iter().map(|b| format!("{b:02x}")).collect();
    match n2n::VersionData::decode(params) {
        Ok(d) => format!(
            "magic={} initiatorOnly={} peerSharing={} query={} v16_flag={:?}  hex={hex}",
            d.network_magic, d.initiator_only_diffusion_mode, d.peer_sharing, d.query, d.v16_flag
        ),
        Err(e) => format!("(undecodable: {e})  hex={hex}"),
    }
}

/// Parse a single hex byte string (even length) into bytes.
fn parse_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("hex string has odd length: {s}"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| format!("bad hex '{s}': {e}")))
        .collect()
}

pub async fn run(
    host: &str,
    opts: Options,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = host
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("could not resolve {host}"))?;

    // Base version data we advertise for every proposed version.
    let base = n2n::VersionData {
        network_magic: opts.magic,
        initiator_only_diffusion_mode: opts.initiator_only,
        peer_sharing: opts.peer_sharing,
        query: opts.query,
        v16_flag: None,
    };

    // Which versions to propose.
    let mut versions = if opts.propose.is_empty() {
        n2n::version_table(&base)
    } else {
        n2n::version_table_for(&base, &opts.propose)
    };

    // Apply raw "N=HEX" overrides (bypass the encoder for probing).
    for spec in &opts.raw_version {
        let (v, hex) = spec
            .split_once('=')
            .ok_or_else(|| format!("--raw-version expects N=HEX, got '{spec}'"))?;
        let version: u64 = v.parse().map_err(|e| format!("bad version '{v}': {e}"))?;
        versions.insert(version, parse_hex(hex)?);
    }

    let proposed: Vec<u64> = versions.keys().copied().collect();
    println!("connecting to {addr}...");
    println!(
        "proposing versions {proposed:?}  (query={}, initiatorOnly={}, peerSharing={})",
        opts.query, opts.initiator_only, opts.peer_sharing
    );

    let bearer = TcpBearer::connect(addr).await?;

    let proto = ProtocolConfig {
        id: handshake::PROTOCOL_ID,
        traffic_class: TrafficClass::Priority,
        ingress_limit: handshake::SIZE_LIMIT,
        egress_queue_size: 4,
    };

    let mut mux = Mux::new(MuxConfig::default(), RoundRobin::default(), MODE_INITIATOR);
    let (send_ch, recv_ch) = mux.register(&proto);
    let running = mux.run(bearer);

    let result =
        handshake::run_client(CodecSend::new(send_ch), CodecRecv::new(recv_ch), versions).await;

    match &result {
        Ok(handshake::HandshakeResult::Accepted {
            version_number,
            params,
        }) => {
            println!("handshake ACCEPTED: version {version_number}");
            println!("  negotiated: {}", describe(params));
        }
        Ok(handshake::HandshakeResult::Refused(reason)) => {
            println!("handshake REFUSED: {reason:?}");
        }
        Ok(handshake::HandshakeResult::QueryReply(table)) => {
            let mut vers: Vec<u64> = table.keys().copied().collect();
            vers.sort_unstable();
            println!("QUERY REPLY: {} versions {vers:?}", table.len());
            for v in &vers {
                if let Some(params) = table.get(v) {
                    println!("  v{v}: {}", describe(params));
                }
            }
        }
        Err(e) => {
            println!("handshake error: {e}");
        }
    }

    running.abort();
    Ok(())
}
