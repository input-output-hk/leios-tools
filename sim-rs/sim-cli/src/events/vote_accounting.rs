//! Shared vote-wire classification for the trace aggregator and traffic report.
//! Sizes travel with both send and receive events, so accounting never depends
//! on observing a separate generation event first.
use sim_core::events::{Event, Node};

#[derive(Clone, Copy)]
pub(super) enum VoteMessageKind {
    Body,
    Announcement,
    Request,
}

pub(super) struct VoteMessage<'a> {
    pub node: &'a Node,
    pub receiving: bool,
    pub kind: VoteMessageKind,
    pub bytes: u64,
}

pub(super) fn message(event: &Event) -> Option<VoteMessage<'_>> {
    use VoteMessageKind::*;
    let (node, receiving, kind, bytes) = match event {
        Event::VTBundleSent {
            sender,
            msg_size_bytes,
            ..
        } => (sender, false, Body, *msg_size_bytes),
        Event::VTBundleReceived {
            recipient,
            msg_size_bytes,
            ..
        } => (recipient, true, Body, *msg_size_bytes),
        Event::VTBundleAnnounced {
            sender,
            msg_size_bytes,
            ..
        } => (sender, false, Announcement, *msg_size_bytes),
        Event::VTBundleAnnouncementReceived {
            recipient,
            msg_size_bytes,
            ..
        } => (recipient, true, Announcement, *msg_size_bytes),
        Event::VTBundleRequested {
            sender,
            msg_size_bytes,
            ..
        } => (sender, false, Request, *msg_size_bytes),
        Event::VTBundleRequestReceived {
            recipient,
            msg_size_bytes,
            ..
        } => (recipient, true, Request, *msg_size_bytes),
        // Duplicate/obsolete classifications are subsets, not more wire bytes.
        _ => return None,
    };
    Some(VoteMessage {
        node,
        receiving,
        kind,
        bytes,
    })
}
