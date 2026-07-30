mod blockfetch;
mod capture;
mod chainsync;
mod connect;
mod follow;
mod handshake;
mod multi_follow;
mod peershare;
mod scheduler_args;
mod serve;
mod submit;

use clap::{Parser, Subcommand};
use net_core::protocols::handshake::n2n;
use scheduler_args::SchedulerArgs;

#[derive(Parser)]
#[command(name = "net-cli", about = "Cardano network protocol test tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Connect to a node and perform a version handshake
    Handshake {
        /// Host and port to connect to (e.g., relay:3001)
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Send a query handshake: ask the peer for its full version table
        /// and print it, without committing to a version.
        #[arg(long)]
        query: bool,

        /// Advertise initiator-only diffusion (default: duplex /
        /// initiator-and-responder). Some nodes refuse initiator-only peers.
        #[arg(long)]
        initiator_only: bool,

        /// PeerSharing flag to advertise (0 or 1).
        #[arg(long, default_value_t = 1)]
        peer_sharing: u8,

        /// Comma-separated versions to propose (e.g. "14,15,16"). Defaults to
        /// all supported versions.
        #[arg(long, value_delimiter = ',')]
        propose: Vec<u64>,

        /// Override a version's raw params with hex, as "N=HEX" (repeatable).
        /// Bypasses the encoder to probe non-standard version-data encodings.
        #[arg(long = "raw-version", value_name = "N=HEX")]
        raw_version: Vec<String>,
    },

    /// Capture raw handshake bytes from a node for test vectors
    Capture {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,
    },

    /// Follow the chain tip via ChainSync (limited count, for debugging)
    ChainSync {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Number of headers to follow
        #[arg(long, default_value_t = 20)]
        count: usize,
    },

    /// Fetch blocks via BlockFetch (uses ChainSync to find tip first,
    /// or a supplied point with --slot/--hash).
    BlockFetch {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Override: target slot for BlockFetch (must be paired with --hash).
        #[arg(long)]
        slot: Option<u64>,

        /// Override: target 32-byte block hash in hex (must be paired with --slot).
        #[arg(long)]
        hash: Option<String>,

        /// Print the fetched block as hex on stdout.
        #[arg(long)]
        dump_hex: bool,
    },

    /// Run a fake Cardano node serving synthetic blocks
    Serve {
        /// Port to listen on
        #[arg(long, default_value_t = 3001)]
        port: u16,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Block generation rate (blocks/sec, Poisson λ)
        #[arg(long, default_value_t = 0.05)]
        block_rate: f64,

        /// Rollback rate (rollbacks/sec, Poisson λ; 0 = no rollbacks)
        #[arg(long, default_value_t = 0.0)]
        rollback_rate: f64,

        /// Maximum rollback depth (capped at chain length - 1)
        #[arg(long, default_value_t = 3)]
        max_rollback_depth: usize,

        /// Enable Leios protocols (LeiosNotify + LeiosFetch with synthetic EB/vote generation)
        #[arg(long)]
        leios: bool,

        /// Maximum concurrent inbound handshakes
        #[arg(long, default_value_t = 64)]
        max_handshaking: usize,

        /// Maximum connections (handshaking + established) per IP
        #[arg(long, default_value_t = 3)]
        max_connections_per_ip: usize,

        #[command(flatten)]
        scheduler_args: SchedulerArgs,
    },

    /// Submit random transactions via TxSubmission
    Submit {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Tx generation rate (per second, Poisson). Omit for single tx.
        #[arg(long)]
        tx_rate: Option<f64>,

        /// Minimum tx body size in bytes
        #[arg(long, default_value_t = 1500)]
        min_size: usize,

        /// Maximum tx body size in bytes
        #[arg(long, default_value_t = 1500)]
        max_size: usize,

        /// Number of transactions to submit (default: 1 if no --tx-rate, infinite otherwise)
        #[arg(long)]
        count: Option<usize>,
    },

    /// Request peers from a node via PeerSharing
    PeerShare {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Number of peers to request
        #[arg(long, default_value_t = 10)]
        amount: u8,
    },

    /// Follow the chain tip continuously with reconnection
    Follow {
        /// Host and port to connect to
        host: String,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Maximum rollback depth (number of points to retain)
        #[arg(long, default_value_t = 2160)]
        max_rollback: usize,

        #[command(flatten)]
        scheduler_args: SchedulerArgs,
    },

    /// Follow the chain tip from multiple peers via the coordinator
    MultiFollow {
        /// Hosts to connect to (repeatable)
        #[arg(long = "host")]
        hosts: Vec<String>,

        /// Network magic number
        #[arg(long, default_value_t = n2n::MAINNET_MAGIC)]
        magic: u64,

        /// Maximum number of peers
        #[arg(long, default_value_t = 20)]
        max_peers: usize,

        /// Listen address for inbound peers (e.g. 0.0.0.0:3001)
        #[arg(long)]
        listen: Option<String>,

        /// Use duplex mode (both client and server protocols per connection)
        #[arg(long)]
        duplex: bool,

        /// Enable Leios protocols (LeiosNotify + LeiosFetch)
        #[arg(long)]
        leios: bool,

        /// Dump raw CBOR hex of each received Leios mini-protocol message to
        /// stderr (`WIRE_HEX recv …`), for capturing wire test vectors.
        #[arg(long)]
        wire_hex: bool,

        /// On each LeiosBlockOffer, issue a LeiosFetch MsgLeiosBlockRequest for
        /// the offered EB (so its MsgLeiosBlock reply can be captured).
        #[arg(long)]
        fetch_eb: bool,

        /// On each LeiosBlockTxsOffer, issue a MsgLeiosBlockTxsRequest for the
        /// first tx chunk (so its MsgLeiosBlockTxs reply can be captured).
        #[arg(long)]
        fetch_eb_txs: bool,

        /// Maximum concurrent inbound handshakes
        #[arg(long, default_value_t = 64)]
        max_handshaking: usize,

        /// Maximum connections (handshaking + established) per IP
        #[arg(long, default_value_t = 3)]
        max_connections_per_ip: usize,

        #[command(flatten)]
        scheduler_args: SchedulerArgs,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Command::Handshake {
            host,
            magic,
            query,
            initiator_only,
            peer_sharing,
            propose,
            raw_version,
        } => {
            handshake::run(
                &host,
                handshake::Options {
                    magic,
                    query,
                    initiator_only,
                    peer_sharing,
                    propose,
                    raw_version,
                },
            )
            .await
        }
        Command::Capture { host, magic } => capture::run(&host, magic).await,
        Command::ChainSync { host, magic, count } => chainsync::run(&host, magic, count).await,
        Command::BlockFetch {
            host,
            magic,
            slot,
            hash,
            dump_hex,
        } => {
            let point = match (slot, hash) {
                (Some(slot), Some(hash_hex)) => {
                    // Reject odd-length / non-64-char input up front; the
                    // slice path below would otherwise panic on
                    // `&hash_hex[i..i+2]` when the last 2-byte window
                    // overruns the string.
                    let hash_hex = hash_hex.strip_prefix("0x").unwrap_or(&hash_hex);
                    if hash_hex.len() != 64 {
                        return Err(format!(
                            "--hash must be 64 hex chars (32 bytes); got {}",
                            hash_hex.len()
                        )
                        .into());
                    }
                    let bytes = (0..hash_hex.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&hash_hex[i..i + 2], 16))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| format!("--hash hex: {e}"))?;
                    let mut h = [0u8; 32];
                    h.copy_from_slice(&bytes);
                    Some(net_core::types::Point::Specific { slot, hash: h })
                }
                (None, None) => None,
                _ => return Err("must supply both --slot and --hash, or neither".into()),
            };
            blockfetch::run(&host, magic, point, dump_hex).await
        }
        Command::Serve {
            port,
            magic,
            block_rate,
            rollback_rate,
            max_rollback_depth,
            leios,
            max_handshaking,
            max_connections_per_ip,
            scheduler_args,
        } => {
            serve::run(
                port,
                magic,
                block_rate,
                rollback_rate,
                max_rollback_depth,
                leios,
                max_handshaking,
                max_connections_per_ip,
                &scheduler_args,
            )
            .await
        }
        Command::Submit {
            host,
            magic,
            tx_rate,
            min_size,
            max_size,
            count,
        } => submit::run(&host, magic, tx_rate, min_size, max_size, count).await,
        Command::PeerShare {
            host,
            magic,
            amount,
        } => peershare::run(&host, magic, amount).await,
        Command::Follow {
            host,
            magic,
            max_rollback,
            scheduler_args,
        } => follow::run(&host, magic, max_rollback, &scheduler_args).await,
        Command::MultiFollow {
            hosts,
            magic,
            max_peers,
            listen,
            duplex,
            leios,
            wire_hex,
            fetch_eb,
            fetch_eb_txs,
            max_handshaking,
            max_connections_per_ip,
            scheduler_args,
        } => {
            net_core::mux::codec::set_wire_hex(wire_hex);
            multi_follow::run(
                &hosts,
                magic,
                max_peers,
                listen,
                duplex,
                leios,
                fetch_eb,
                fetch_eb_txs,
                max_handshaking,
                max_connections_per_ip,
                &scheduler_args,
            )
            .await
        }
    }
}
