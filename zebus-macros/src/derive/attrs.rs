use proc_macro2::Span;
use syn::{spanned::Spanned, Attribute, Lit, Meta, MetaList, MetaNameValue, NestedMeta};

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
            macro_rules! attr {
                ($name:ident: str) => {
                    if let Meta::NameValue(MetaNameValue {
                        ref path, ref lit, ..
                    }) = meta
                    {
                        if path.is_ident(stringify!($name)) {
                            attrs.$name = Some(if let Lit::Str(s) = lit {
                                s.value()
                            } else {
                                return Err(syn::Error::new_spanned(
                                    path,
                                    format!(
                                        "invalid value for `{}` expected: string got: {:?}",
                                        stringify!($name),
                                        lit
                                    ),
                                ));
                            });
                        }
                    }
                };

                ($name:ident: bool) => {
                    match meta {
                        Meta::NameValue(MetaNameValue {
                            ref path, ref lit, ..
                        }) => {
                            if path.is_ident(stringify!($name)) {
                                attrs.$name = Some(if let Lit::Bool(s) = lit {
                                    s.value()
                                } else {
                                    return Err(syn::Error::new_spanned(
                                        path,
                                        format!(
                                            "invalid value for `{}` expected: bool got: {:?}",
                                            stringify!($name),
                                            lit
                                        ),
                                    ));
                                });
                            }
                        }
                        Meta::Path(ref path) => {
                            if path.is_ident(stringify!($name)) {
                                attrs.$name = Some(true);
                            }
                        }
                        _ => {}
                    }
                };
            }

            attr!(namespace: str);
            attr!(infrastructure: bool);
            attr!(transient: bool);
            attr!(routable: bool);
            attr!(dispatch_queue: str);
        }

        Ok(attrs)
    }
}

impl TryFrom<Vec<Meta>> for ZebusFieldAttrs {
    type Error = syn::Error;

    fn try_from(value: Vec<Meta>) -> Result<Self, Self::Error> {
        let mut attrs = ZebusFieldAttrs::default();

        for meta in value {
            match meta {
                Meta::NameValue(MetaNameValue {
                    ref path, ref lit, ..
                }) => {
                    if path.is_ident("routing_position") {
                        attrs.routing_position = Some(if let Lit::Int(int) = lit {
                            int.base10_parse::<usize>()?
                        } else {
                            return Err(
                                syn::Error::new_spanned(path, format!("invalid value for `routing_position` expected: int got: {lit:?}"))
                            );
                        });
                    }
                }
                _ => {}
            }
        }

        Ok(attrs)
    }
}

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
