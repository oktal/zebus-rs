//! Prefix tree of peers indexing peers by their [`BindingKey`] bindings
//!
//! This module provides a [`PeerSubscriptionTree`] that can be used to organize peers
//! as a tree based on their [`BindingKey`] bindings (subscriptions)
//!
//! For example the following peer bindings
//!
//! | Peer           | Binding key            |
//! |----------------|------------------------|
//! | Peer.0         | france                 |
//! | Peer.1         | france.october.*       |
//! | Peer.2         | *.june.21              |
//! | Peer.3         | belgium.*              |
//!
//! will be organized as such in the tree:
//!
//! ```text
//!    * (0) []
//!        june (1) []
//!            21 (2) [Peer(Peer.2, tcp://*:*)]
//!    france (0) [Peer(Peer.0, tcp://*:*)]
//!        october (1) []
//!            * (2) [Peer(Peer.1, tcp://*:*)]
//!    belgium (0) []
//!        * (1) [Peer(Peer.3, tcp://*:*)]
//! ```
use crate::{BindingKey, Peer};
use std::fmt;
use zebus_core::BindingKeyFragment;

/// Type used to walk the tree and collect peers
struct PeerCollector {
    /// Peers that were collected during tree walking
    peers: Vec<Peer>,
}

impl PeerCollector {
    /// Create a new empty collector
    fn new() -> Self {
        Self { peers: vec![] }
    }

    /// Offer [`Peer`] peers to this collector
    fn offer(&mut self, peers: impl IntoIterator<Item = Peer>) {
        self.peers.extend(peers.into_iter());
    }

    /// Consumes the [`PeerCollector`] and return the list of peers collected
    fn into_peers(self) -> Vec<Peer> {
        self.peers
    }
}

fn add_or_update_peer(peers: &mut Vec<Peer>, peer: Peer) {
    let existing_peer = peers.iter_mut().find(|p| p.id == peer.id);

    if let Some(p) = existing_peer {
        *p = peer;
    } else {
        peers.push(peer)
    }
}

fn remove_peer(peers: &mut Vec<Peer>, peer: &Peer) {
    peers.retain(|p| p.id != peer.id);
}

/// Tree node visitor. Behavior to apply when walking down the tree to find a [`Node`]
/// for a fragment of a [`BindingKey`]
trait NodeVisitor {
    fn visit(key: &BindingKey, index: usize) -> Option<Box<Node>>;
}

/// Create a new node for a fragment of a [`BindingKey`]
struct CreateNode;

/// Visit that does not do anything
struct NoopVisitor;

impl NodeVisitor for CreateNode {
    fn visit(key: &BindingKey, index: usize) -> Option<Box<Node>> {
        Some(Box::new(Node::new(index, key.fragment(index).cloned())))
    }
}

impl NodeVisitor for NoopVisitor {
    fn visit(_key: &BindingKey, _index: usize) -> Option<Box<Node>> {
        None
    }
}

/// A node of the tree
#[derive(Debug, Default)]
struct Node {
    /// Special node to represent the `*` binding key fragment
    star_node: Option<Box<Node>>,

    /// Special node to represent the `#` binding key fragment
    sharp_node: Option<Box<Node>>,

    /// Children of this node
    children: Vec<Box<Node>>,

    /// The current index of the binding key fragment that this node holds
    /// Will be 0 for the root node
    fragment_index: usize,

    /// The current fragment of the binding key that this node holds
    /// Will be `None` for the root node
    fragment: Option<BindingKeyFragment>,

    /// Peers that match this node
    peers: Vec<Peer>,
}

impl Node {
    /// Create a new [`Node`] with a fragment of a binding key
    fn new(fragment_index: usize, fragment: Option<BindingKeyFragment>) -> Self {
        Self {
            fragment_index,
            fragment,
            ..Default::default()
        }
    }

    /// Create a special root node
    fn root() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Walk down the tree to find the final node for a fragment of a binding key
    /// Returns a mutable reference of the list of peers from the node that was found or `None`
    /// otherwise
    fn find<'a, V: NodeVisitor>(
        &'a mut self,
        index: usize,
        key: &BindingKey,
    ) -> Option<&'a mut Vec<Peer>> {
        if self.is_leaf(key) {
            return Some(&mut self.peers);
        }

        Self::get_node::<V>(self, key, index).and_then(|n| n.find::<V>(index + 1, key))
    }

    /// Attempt to collect the peers assocaited with this node is the node
    /// is a leaf for a particular binding key
    fn accept(&self, collector: &mut PeerCollector, key: &BindingKey) {
        if self.is_leaf(key) {
            collector.offer(self.peers.clone());
            return;
        }

        if let Some(ref sharp_node) = self.sharp_node {
            sharp_node.offer(collector);
        }

        if let Some(ref star_node) = self.star_node {
            star_node.accept(collector, key);
        }

        if self.children.is_empty() {
            return;
        }

        let child = self.children.iter().find(|n| n.is(key));
        if let Some(child_node) = child {
            child_node.accept(collector, key);
        }
    }

    /// Collect all the peers associated with this node and their children
    fn offer(&self, collector: &mut PeerCollector) {
        collector.offer(self.peers.clone());

        if let Some(ref star_node) = self.star_node {
            star_node.offer(collector);
        }
        if let Some(ref sharp_node) = self.sharp_node {
            sharp_node.offer(collector);
        }

        for child in &self.children {
            child.offer(collector);
        }
    }

    /// Returns `true` if this node holds a fragment corresponding to the `key`
    fn is(&self, key: &BindingKey) -> bool {
        key.fragment(self.fragment_index) == self.fragment.as_ref()
    }

    fn get_node<'a, V: NodeVisitor>(
        &'a mut self,
        key: &BindingKey,
        index: usize,
    ) -> Option<&'a mut Node> {
        key.fragment(index).and_then(|fragment| match fragment {
            BindingKeyFragment::Star => Self::visit_node::<V>(key, index, &mut self.star_node),
            BindingKeyFragment::Sharp => Self::visit_node::<V>(key, index, &mut self.sharp_node),
            BindingKeyFragment::Value(_) => Self::visit_child::<V>(key, index, &mut self.children),
        })
    }

    fn visit_node<'a, V: NodeVisitor>(
        key: &BindingKey,
        index: usize,
        node: &'a mut Option<Box<Node>>,
    ) -> Option<&'a mut Node> {
        match node {
            Some(n) => Some(n.as_mut()),
            None => {
                *node = V::visit(key, index);
                node.as_mut().map(|n| n.as_mut())
            }
        }
    }

    fn visit_child<'a, V: NodeVisitor>(
        key: &BindingKey,
        index: usize,
        children: &'a mut Vec<Box<Node>>,
    ) -> Option<&'a mut Node> {
        let fragment = key.fragment(index);
        match children
            .iter()
            .position(|n| n.fragment.as_ref() == fragment)
        {
            Some(i) => Some(&mut children[i]),
            None => V::visit(key, index).map(|node| {
                children.push(node);
                children
                    .last_mut()
                    .expect("children should have at least one element")
                    .as_mut()
            }),
        }
    }

    fn is_leaf(&self, key: &BindingKey) -> bool {
        if self.fragment.is_none() {
            return false;
        }

        self.fragment_index == key.len() - 1
    }
}

/// Indentation level to use when printing the tree
const INDENT_LEVEL: usize = 4;

/// Helper type to display the list of peers associated with a [`Node`] of the tree
struct PeersDisplay<'a>(&'a Vec<Peer>);

impl fmt::Display for PeersDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        for (idx, peer) in self.0.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{peer}")?;
        }

        write!(f, "]")?;
        Ok(())
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref fragment) = self.fragment {
            let fragment_index = self.fragment_index;
            let peers = PeersDisplay(&self.peers);
            let indent = fragment_index * INDENT_LEVEL;
            writeln!(f, "{:indent$}{fragment} ({fragment_index}) {peers}", "")?;
        }

        if let Some(ref star_node) = self.star_node {
            write!(f, "{star_node}")?;
        }

        if let Some(ref sharp_node) = self.sharp_node {
            write!(f, "{sharp_node}")?;
        }

        for child in &self.children {
            write!(f, "{child}")?;
        }

        Ok(())
    }
}

/// Prefix tree of peers based on their [`BindingKey`] bindings
#[derive(Debug)]
pub(crate) struct PeerSubscriptionTree {
    root: Box<Node>,
    root_peers: Vec<Peer>,
}

impl PeerSubscriptionTree {
    /// Create a new empty tree
    pub(crate) fn new() -> Self {
        Self {
            root: Box::new(Node::root()),
            root_peers: vec![],
        }
    }

    /// Add a peer [`Peer`] with a binding [`BindingKey`] to the tree
    pub(crate) fn add(&mut self, peer: Peer, key: &BindingKey) {
        let peers = self
            .find::<CreateNode>(key)
            .expect("`find` should have created node");
        add_or_update_peer(peers, peer);
    }

    /// Remove a peer [`Peer`] with a binding [`BindingKey`] from the tree
    pub(crate) fn remove(&mut self, peer: &Peer, key: &BindingKey) {
        if let Some(peers) = self.find::<NoopVisitor>(key) {
            remove_peer(peers, peer);
        }
    }

    /// Get the list of peers that match a binding [`BindingKey`]
    /// TODO(oktal): Support RoutingContent
    pub(crate) fn get_peers(&self, key: &BindingKey) -> Vec<Peer> {
        let mut collector = PeerCollector::new();
        collector.offer(self.root_peers.clone());

        if key.is_empty() {
            self.root.offer(&mut collector);
        } else {
            self.root.accept(&mut collector, key);
        }

        collector.into_peers()
    }

    fn find<'a, V: NodeVisitor>(&'a mut self, key: &BindingKey) -> Option<&'a mut Vec<Peer>> {
        if key.is_empty() {
            Some(&mut self.root_peers)
        } else {
            self.root.find::<V>(0, key)
        }
    }
}

impl fmt::Display for PeerSubscriptionTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.root)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zebus_core::binding_key;

    #[test]
    fn match_empty() {
        let peer = Peer::test();
        let mut tree = PeerSubscriptionTree::new();
        tree.add(peer.clone(), &BindingKey::empty());

        let peers = tree.get_peers(&BindingKey::empty());
        assert_eq!(peers.get(0), Some(&peer))
    }

    #[test]
    fn match_basic() {
        let peer = Peer::test();
        let mut tree = PeerSubscriptionTree::new();
        tree.add(peer.clone(), &binding_key!["my_routing"].into());

        let peers = tree.get_peers(&binding_key!["my_routing"].into());
        assert_eq!(peers.get(0), Some(&peer));
    }

    #[test]
    fn star_always_match() {
        for key in [binding_key!["routing"], binding_key![*], binding_key![#]] {
            let routing_key = key.into();

            let peer = Peer::test();
            let mut tree = PeerSubscriptionTree::new();
            tree.add(peer.clone(), &binding_key![*].into());

            let peers = tree.get_peers(&routing_key);
            assert_eq!(peers.get(0), Some(&peer))
        }
    }

    #[test]
    fn empty_key_returns_all() {
        let (peer1, peer2, peer3) = (Peer::test(), Peer::test(), Peer::test());

        let mut tree = PeerSubscriptionTree::new();
        tree.add(peer1.clone(), &binding_key!["my_routing"].into());
        tree.add(peer2.clone(), &binding_key!["my_routing", *, 456].into());
        tree.add(
            peer3.clone(),
            &binding_key!["my_other_routing", "september", *].into(),
        );

        let peers = tree.get_peers(&BindingKey::empty());
        assert_eq!(peers.len(), 3);
    }

    #[test]
    fn star_matches_equal_parts() {
        let peer = Peer::test();
        let mut tree = PeerSubscriptionTree::new();

        tree.add(peer.clone(), &binding_key![*, *, *].into());

        for routing_key in [binding_key!["a", "b", "c"], binding_key!["d", "e", "f"]] {
            let peers = tree.get_peers(&routing_key.into());
            assert_eq!(peers.get(0), Some(&peer));
        }
    }

    #[test]
    fn star_matches_anything() {
        for binding_key in [
            binding_key!["a", "b", *],
            binding_key!["a", *, *],
            binding_key!["a", *, "c"],
            binding_key![*, "b", "c"],
        ] {
            let peer = Peer::test();

            let mut tree = PeerSubscriptionTree::new();
            tree.add(peer.clone(), &binding_key.into());

            let peers = tree.get_peers(&binding_key!["a", "b", "c"].into());
            assert_eq!(peers.get(0), Some(&peer));
        }
    }

    #[test]
    fn dash_matches() {
        for binding_key in [binding_key!["a", "b", #], binding_key!["a", #]] {
            let peer = Peer::test();
            let mut tree = PeerSubscriptionTree::new();

            tree.add(peer.clone(), &binding_key.into());

            let peers = tree.get_peers(&binding_key!["a", "b", "c"].into());
            assert_eq!(peers.get(0), Some(&peer));
        }
    }

    #[test]
    fn match_peers() {
        let (peer1, peer2, peer3) = (Peer::test(), Peer::test(), Peer::test());

        let mut tree = PeerSubscriptionTree::new();
        tree.add(peer1.clone(), &binding_key!["my_routing", *, 456].into());
        tree.add(peer2.clone(), &binding_key!["my_routing", *, *].into());
        tree.add(
            peer3.clone(),
            &binding_key!["my_other_routing", 789, *].into(),
        );

        let peers = tree.get_peers(&binding_key!["my_routing", "monday", 456].into());
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0], peer2);
        assert_eq!(peers[1], peer1);
    }

    #[test]
    fn remove_basic() {
        let peer = Peer::test();
        let mut tree = PeerSubscriptionTree::new();

        let binding_key = binding_key!["my_routing", *, "test"].into();
        tree.add(peer.clone(), &binding_key);
        tree.remove(&peer, &binding_key);

        let peers = tree.get_peers(&binding_key);
        assert_eq!(peers.len(), 0);
    }
}
