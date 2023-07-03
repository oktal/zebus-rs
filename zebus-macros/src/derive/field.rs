use super::attrs::ZebusFieldAttrs;
use crate::attrs::find_attrs;

pub(crate) enum Field {
    Normal(syn::Field),

    Routed { field: syn::Field, position: usize },
}

impl TryFrom<syn::Field> for Field {
    type Error = syn::Error;

    fn try_from(field: syn::Field) -> Result<Self, Self::Error> {
        let attrs: ZebusFieldAttrs = find_attrs(&field.attrs[..], "zebus")?;

        if let Some(position) = attrs.routing_position {
            Ok(Field::Routed { field, position })
        } else {
            Ok(Field::Normal(field))
        }
    }
}
