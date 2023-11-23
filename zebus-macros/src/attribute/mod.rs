use std::str::FromStr;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemImpl, NestedMeta, PathArguments};
use zebus_core::{BindingKey, BindingKeyFragment};

#[derive(Debug, Default, deluxe::ParseMetaItem)]
#[deluxe(default)]
pub struct HandlerAttrs {
    #[deluxe(flatten)]
    mode: SubscriptionModeAttr,

    #[deluxe(append, rename = binding)]
    bindings: Vec<String>,
}

#[derive(Debug, Default, deluxe::ParseMetaItem)]
#[deluxe(default)]
enum SubscriptionModeAttr {
    #[default]
    Auto,
    Manual,
}

fn expand(item: ItemImpl, attrs: HandlerAttrs) -> syn::Result<TokenStream> {
    let trait_ = item.trait_.clone().ok_or(syn::Error::new_spanned(
        &item,
        "expected trait implementation",
    ))?;
    let trait_path = trait_.1;

    let handler_segment = trait_path
        .segments
        .iter()
        .find(|s| s.ident == "Handler")
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

    let mode = match attrs.mode {
        SubscriptionModeAttr::Auto => quote! { ::zebus_core::SubscriptionMode::Auto },
        SubscriptionModeAttr::Manual => quote! { ::zebus_core::SubscriptionMode::Manual },
    };

    let ty = &item.self_ty;

    let bindings = attrs.bindings.iter().map(|binding| {
        let binding = BindingKey::from_str(binding).unwrap();
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
    let attrs = match deluxe::parse::<HandlerAttrs>(attr) {
        Ok(attrs) => attrs,
        Err(e) => return e.into_compile_error().into(),
    };

    let item_impl = parse_macro_input!(item as ItemImpl);
    expand(item_impl, attrs).unwrap_or_else(|e| e.into_compile_error().into())
}
