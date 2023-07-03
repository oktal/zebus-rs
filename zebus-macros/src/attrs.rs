use std::{fmt::Debug, str::FromStr};

use syn::{Attribute, Lit, Meta, MetaList, MetaNameValue, NestedMeta};

pub(crate) fn find_attrs<Attrs>(attributes: &[Attribute], ident: &str) -> syn::Result<Attrs>
where
    Attrs: TryFrom<Vec<Meta>, Error = syn::Error>,
{
    let attrs = attributes
        .iter()
        .flat_map(Attribute::parse_meta)
        .flat_map(|meta| match meta {
            Meta::List(MetaList { path, nested, .. }) => {
                if path.is_ident(ident) {
                    nested.into_iter().collect()
                } else {
                    vec![]
                }
            }
            _ => vec![],
        })
        .flat_map(|nested| -> Result<_, _> {
            match nested {
                NestedMeta::Meta(attr) => Ok(attr),
                x => Err(syn::Error::new_spanned(x, "invalid attribute")),
            }
        })
        .collect();

    Attrs::try_from(attrs)
}

pub(crate) fn attr<T>(name: &str, meta: &Meta) -> Result<Option<T>, syn::Error>
where
    T: FromStr,
    T::Err: Debug,
{
    if let Meta::NameValue(MetaNameValue {
        ref path, ref lit, ..
    }) = meta
    {
        if path.is_ident(name) {
            if let Lit::Str(s) = lit {
                let value = s.value().parse().map_err(|e| {
                    syn::Error::new_spanned(path, format!("invalid value for `{}`: {:?}", name, e))
                })?;

                return Ok(Some(value));
            } else {
                return Err(syn::Error::new_spanned(
                    path,
                    format!(
                        "invalid value for `{}` expected: string got: {:?}",
                        name, lit
                    ),
                ));
            }
        }
    }

    Ok(None)
}

pub(crate) fn attr_str(
    name: &str,
    value: &mut Option<String>,
    meta: &Meta,
) -> Result<(), syn::Error> {
    if let Meta::NameValue(MetaNameValue {
        ref path, ref lit, ..
    }) = meta
    {
        if path.is_ident(name) {
            *value = Some(if let Lit::Str(s) = lit {
                s.value()
            } else {
                return Err(syn::Error::new_spanned(
                    path,
                    format!(
                        "invalid value for `{}` expected: string got: {:?}",
                        name, lit
                    ),
                ));
            });
        }
    }

    Ok(())
}

pub(crate) fn attr_bool(
    name: &str,
    value: &mut Option<bool>,
    meta: &Meta,
) -> Result<(), syn::Error> {
    match meta {
        Meta::NameValue(MetaNameValue {
            ref path, ref lit, ..
        }) => {
            if path.is_ident(name) {
                *value = Some(if let Lit::Bool(s) = lit {
                    s.value()
                } else {
                    return Err(syn::Error::new_spanned(
                        path,
                        format!("invalid value for `{}` expected: bool got: {:?}", name, lit),
                    ));
                });
            }
        }
        Meta::Path(ref path) => {
            if path.is_ident(name) {
                *value = Some(true);
            }
        }
        _ => {}
    }

    Ok(())
}

pub(crate) fn attr_int<N>(name: &str, value: &mut Option<N>, meta: &Meta) -> Result<(), syn::Error>
where
    N: FromStr,
    N::Err: std::fmt::Display,
{
    if let Meta::NameValue(MetaNameValue {
        ref path, ref lit, ..
    }) = meta
    {
        if path.is_ident(name) {
            *value = Some(if let Lit::Int(int) = lit {
                int.base10_parse::<N>()?
            } else {
                return Err(syn::Error::new_spanned(
                    path,
                    format!("invalid value for `{}` expected: int got: {:?}", name, lit),
                ));
            });
        }
    }

    Ok(())
}
