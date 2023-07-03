use proc_macro2::Span;
use syn::{spanned::Spanned, Meta};

use crate::attrs::{attr_bool, attr_int, attr_str};

#[derive(Debug)]
pub struct ZebusStructAttrs {
    pub namespace: Option<String>,
    pub infrastructure: Option<bool>,
    pub transient: Option<bool>,
    pub routable: Option<bool>,
    pub span: Option<Span>,
    pub dispatch_queue: Option<String>,
}

#[derive(Debug, Default)]
pub struct ZebusFieldAttrs {
    pub routing_position: Option<usize>,
}

impl TryFrom<Vec<Meta>> for ZebusStructAttrs {
    type Error = syn::Error;

    fn try_from(value: Vec<Meta>) -> Result<Self, Self::Error> {
        let mut attrs = ZebusStructAttrs {
            span: value.get(0).map(|m| m.span()),
            namespace: None,
            infrastructure: None,
            transient: None,
            routable: None,
            dispatch_queue: None,
        };

        for meta in value {
            attr_str("namespace", &mut attrs.namespace, &meta)?;
            attr_bool("infrastructure", &mut attrs.infrastructure, &meta)?;
            attr_bool("transient", &mut attrs.transient, &meta)?;
            attr_bool("routable", &mut attrs.routable, &meta)?;
            attr_str("dispatch_queue", &mut attrs.dispatch_queue, &meta)?;
        }

        Ok(attrs)
    }
}

impl TryFrom<Vec<Meta>> for ZebusFieldAttrs {
    type Error = syn::Error;

    fn try_from(value: Vec<Meta>) -> Result<Self, Self::Error> {
        let mut attrs = ZebusFieldAttrs::default();

        for meta in value {
            attr_int("routing_position", &mut attrs.routing_position, &meta)?;
        }

        Ok(attrs)
    }
}
