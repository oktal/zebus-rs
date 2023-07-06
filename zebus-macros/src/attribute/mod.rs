use std::str::FromStr;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, AttributeArgs, ItemImpl, Meta, NestedMeta, PathArguments};
use zebus_core::{BindingKey, BindingKeyFragment, SubscriptionMode};

struct SubscriptionModeAttr(SubscriptionMode);

#[derive(Debug)]
pub struct HandlerAttrs {
    pub mode: Option<SubscriptionMode>,
    pub bindings: Vec<BindingKey>,
}

impl TryFrom<Vec<Meta>> for HandlerAttrs {
    type Error = syn::Error;

    fn try_from(value: Vec<Meta>) -> Result<Self, Self::Error> {
        let mut attrs = HandlerAttrs {
            mode: None,
            bindings: vec![],
        };

        for meta in value {
            if let Meta::Path(path) = &meta {
                if path.is_ident("auto") {
                    attrs.mode = Some(SubscriptionMode::Auto);
                } else if path.is_ident("manual") {
                    attrs.mode = Some(SubscriptionMode::Manual);
                } else {
                    return Err(syn::Error::new_spanned(
                        path,
                        "invalid subscription mode. expected `auto` or `manual`",
                    ));
                }
            }

            if let Some(mode) = crate::attrs::attr::<SubscriptionModeAttr>("mode", &meta)? {
                attrs.mode = Some(mode.0);
            } else if let Some(binding) = crate::attrs::attr::<BindingKey>("binding", &meta)? {
                attrs.bindings.push(binding);
            }
        }

        Ok(attrs)
    }
}

impl FromStr for SubscriptionModeAttr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Self(SubscriptionMode::Auto)),
            "manual" => Ok(Self(SubscriptionMode::Manual)),
            _ => Err(format!("invalid subscription mode {s}")),
        }
    }
}

fn expand(item: ItemImpl, args: AttributeArgs) -> syn::Result<TokenStream> {
    let attrs: Vec<_> = args
        .into_iter()
        .flat_map(|nested| -> Result<_, _> {
            match nested {
                NestedMeta::Meta(attr) => Ok(attr),
                x => Err(syn::Error::new_spanned(x, "invalid attribute")),
            }
        })
        .collect();

    let trait_ = item.trait_.clone().ok_or(syn::Error::new_spanned(
        &item,
        "expected trait implementation",
    ))?;
    let trait_path = trait_.1;

    let handler_segment = trait_path
        .segments
        .iter()
        .find(|s| s.ident == "Handler" || s.ident == "ContextAwareHandler")
        .ok_or(syn::Error::new_spanned(
            &item,
            "#[handler] must be applied on a `Handler` trait implementation",
        ))?;

    let handler_ty = match &handler_segment.arguments {
        PathArguments::AngleBracketed(args) => Ok(args.args.iter().next().unwrap()),
        _ => Err(syn::Error::new_spanned(
            handler_segment,
            "unexpected handler argument",
        )),
    }?;

    let attrs = HandlerAttrs::try_from(attrs)?;
    let mode = match attrs.mode.unwrap_or(SubscriptionMode::Auto) {
        SubscriptionMode::Auto => quote! { ::zebus_core::SubscriptionMode::Auto },
        SubscriptionMode::Manual => quote! { ::zebus_core::SubscriptionMode::Manual },
    };

    let ty = &item.self_ty;

    let bindings = attrs.bindings.iter().map(|binding| {
        let binding_expanded = if let Some(fragments) = binding.fragments.as_ref() {
            let fragments_expanded = fragments.iter().map(|fragment| match fragment {
                BindingKeyFragment::Value(v) => {
                    quote! { ::zebus_core::BindingKeyFragment::Value(#v.to_string()) }
                }
                BindingKeyFragment::Star => quote! { ::zebus_core::BindingKeyFragment::Star },
                BindingKeyFragment::Sharp => quote! { ::zebus_core::BindingKeyFragment::Sharp },
            });

            quote! {
                ::zebus_core::BindingKey::from_raw_parts(
                    vec![#( #fragments_expanded ) *]
                )
            }
        } else {
            quote! { ::zebus_core::BindingKey::empty() }
        };

        binding_expanded
    });

    let handler_descriptor_expanded = quote! {
        impl ::zebus_core::HandlerDescriptor<#handler_ty> for #ty {
            fn subscription_mode() -> ::zebus_core::SubscriptionMode {
                #mode
            }

            fn bindings() -> Vec<::zebus_core::BindingKey> {
                vec![#( #bindings )*]
            }
        }
    };

    let expanded = quote! {
        #item

        #handler_descriptor_expanded
    };

    Ok(expanded.into())
}

pub(crate) fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item_impl = parse_macro_input!(item as ItemImpl);
    let attrs = parse_macro_input!(attr as AttributeArgs);
    expand(item_impl, attrs).unwrap_or_else(|e| e.into_compile_error().into())
}
