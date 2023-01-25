use crate::Message;
use crate::{proto::IntoProtobuf, BindingKeyFragment};

pub(crate) mod proto {
    use crate::proto::prost;

    #[derive(Clone, prost::Message)]
    pub struct BindingKey {
        #[prost(string, repeated, tag = "1")]
        pub parts: Vec<String>,
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Hash)]
pub struct BindingKey(zebus_core::BindingKey);

impl From<zebus_core::BindingKey> for BindingKey {
    fn from(binding_key: zebus_core::BindingKey) -> Self {
        Self(binding_key)
    }
}

impl BindingKey {
    pub(crate) fn create(message: &dyn Message) -> Self {
        Self(message.get_binding())
    }

    pub fn empty() -> Self {
        Self(Default::default())
    }

    pub fn fragment(&self, index: usize) -> Option<&BindingKeyFragment> {
        if let Some(ref fragments) = self.0.fragments {
            fragments.get(index)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.0.fragments.as_ref().map(|f| f.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<Vec<&str>> for BindingKey {
    fn from(parts: Vec<&str>) -> Self {
        let parts = parts.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        Self(parts.into())
    }
}

impl From<Vec<String>> for BindingKey {
    fn from(parts: Vec<String>) -> Self {
        Self(parts.into())
    }
}

impl From<proto::BindingKey> for BindingKey {
    fn from(key: proto::BindingKey) -> Self {
        Self::from(key.parts)
    }
}

impl IntoProtobuf for BindingKey {
    type Output = proto::BindingKey;

    fn into_protobuf(self) -> Self::Output {
        let parts = if let Some(ref fragments) = self.0.fragments {
            fragments
                .into_iter()
                .map(|fragment| match fragment {
                    BindingKeyFragment::Value(s) => s.clone(),
                    BindingKeyFragment::Star => "*".to_string(),
                    BindingKeyFragment::Sharp => "#".to_string(),
                })
                .collect()
        } else {
            vec![]
        };

        proto::BindingKey { parts }
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::prost;

    use super::*;
    use zebus_core::fragment;

    #[derive(crate::Command, prost::Message, Clone)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct RoutableCommand {
        #[zebus(routing_position = 1)]
        #[prost(string, tag = 1)]
        name: String,

        #[prost(uint32, tag = 2)]
        #[zebus(routing_position = 2)]
        id: u32,
    }

    #[derive(crate::Command, prost::Message, Clone)]
    #[zebus(namespace = "Abc.Test", routable)]
    struct UnorderedRoutableCommand {
        #[zebus(routing_position = 3)]
        #[prost(string, tag = 1)]
        name: String,

        #[zebus(routing_position = 1)]
        #[prost(uint32, tag = 2)]
        id: u32,

        #[zebus(routing_position = 2)]
        #[prost(bool, tag = 3)]
        flag: bool,
    }

    #[test]
    fn default_is_empty() {
        assert_eq!(BindingKey::default().is_empty(), true);
    }

    #[test]
    fn create_from_routable_command() {
        let cmd = RoutableCommand {
            name: "routable_command".into(),
            id: 9087,
        };

        let binding_key = BindingKey::create(&cmd);
        assert_eq!(
            binding_key.fragment(0),
            Some(&fragment!["routable_command"])
        );
        assert_eq!(binding_key.fragment(1), Some(&fragment![9087]));
        assert_eq!(binding_key.fragment(2), None);
        assert_eq!(binding_key, vec!["routable_command", "9087"].into());
    }

    #[test]
    fn create_from_unordered_routable_command() {
        let cmd = UnorderedRoutableCommand {
            name: "unordered_routable_command".into(),
            id: 9087,
            flag: true,
        };

        let binding_key = BindingKey::create(&cmd);
        assert_eq!(
            binding_key.fragment(2),
            Some(&fragment!["unordered_routable_command"])
        );
        assert_eq!(binding_key.fragment(0), Some(&fragment![9087]));
        assert_eq!(binding_key.fragment(1), Some(&fragment![true]));
        assert_eq!(binding_key.fragment(4), None);
        assert_eq!(
            binding_key,
            vec!["9087", "true", "unordered_routable_command"].into()
        );
    }
}
