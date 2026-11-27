//! Ask the EB producer to insert a transaction before a selected target.
//!
//! The leaf only emits typed intent. The caller reads the signed magazines,
//! checks the ordinary EB candidate, and records the resulting EB.

use crate::behaviour::tree::actions::LeafAction;
use crate::behaviour::tree::control::{ControlSignal, EbTxInsertionControl};
use crate::behaviour::tree::env::{ConsensusCtx, TickCtx};
use crate::behaviour::tree::Status;

#[derive(Debug, Clone)]
pub struct InsertEbTransactionBefore {
    insertion: EbTxInsertionControl,
}

impl InsertEbTransactionBefore {
    pub fn new(insert_mag: String, target_mag: String, evidence_dir: String) -> Self {
        Self {
            insertion: EbTxInsertionControl {
                insert_mag,
                target_mag,
                evidence_dir,
            },
        }
    }
}

impl LeafAction<ConsensusCtx, ControlSignal> for InsertEbTransactionBefore {
    fn contribute(&mut self, _ctx: &TickCtx, out: &mut ControlSignal) -> Status {
        out.leios.eb_tx_insertion = Some(self.insertion.clone());
        Status::Running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_only_insertion_intent() {
        let mut action = InsertEbTransactionBefore::new(
            "insert.mag".into(),
            "target.mag".into(),
            "evidence".into(),
        );
        let mut out = ControlSignal::default();
        assert_eq!(
            action.contribute(&TickCtx::default(), &mut out),
            Status::Running
        );
        assert_eq!(
            out.leios.eb_tx_insertion,
            Some(EbTxInsertionControl {
                insert_mag: "insert.mag".into(),
                target_mag: "target.mag".into(),
                evidence_dir: "evidence".into(),
            })
        );
        assert!(!out.requires_tx_production());

        out.praos.suppress_cert = true;
        assert!(out.requires_tx_production());
    }
}
