use std::str::FromStr;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{parse_macro_input, ItemFn};
use zebus_core::{BindingKey, BindingKeyFragment};

#[derive(Debug, Default, deluxe::ParseMetaItem)]
#[deluxe(default)]
pub struct HandlerAttrs {
    #[deluxe(flatten)]
    mode: SubscriptionModeAttr,

    queue: Option<String>,

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

fn expand(mut item: ItemFn, attrs: HandlerAttrs) -> syn::Result<TokenStream> {
    let mode = match attrs.mode {
        SubscriptionModeAttr::Auto => quote! { ::zebus::zebus_core::SubscriptionMode::Auto },
        SubscriptionModeAttr::Manual => quote! { ::zebus::zebus_core::SubscriptionMode::Manual },
    };

    let bindings = attrs.bindings.iter().map(|binding| {
        let binding = BindingKey::from_str(binding).unwrap();
        let binding_expanded = if let Some(fragments) = binding.fragments.as_ref() {
            let fragments_expanded = fragments.iter().map(|fragment| match fragment {
                BindingKeyFragment::Value(v) => {
                    quote! { ::zebus_core::BindingKeyFragment::Value(#v.to_string()) }
                }
                BindingKeyFragment::Star => {
                    quote! { ::zebus::zebus_core::BindingKeyFragment::Star }
                }
                BindingKeyFragment::Sharp => {
                    quote! { ::zebus::zebus_core::BindingKeyFragment::Sharp }
                }
            });

            quote! {
                ::zebus::zebus_core::BindingKey::from_raw_parts(
                    vec![#( #fragments_expanded ), *]
                )
            }
        } else {
            quote! { ::zebus::zebus_core::BindingKey::empty() }
        };

        binding_expanded
    });

    let old_ident = item.sig.ident.clone();
    let new_ident = Ident::new(&format!("{old_ident}_"), item.sig.ident.span());
    item.sig.ident = new_ident.clone();

    let queue = if let Some(queue) = attrs.queue {
        quote! { Some(#queue) }
    } else {
        quote! { None }
    };

    let handler_descriptor_expanded = quote! {
        #[allow(non_camel_case_types)]
        struct #old_ident;

        impl<S> ::zebus::HandlerDescriptor<S> for #old_ident
            where S: Clone + Send + 'static
        {
            type Service = tower::util::BoxService<::zebus::dispatch::InvokeRequest, Option<::zebus::Response>, std::convert::Infallible>;
            type Binding = ::zebus::BindingKey;

            fn service(self, state: S) -> Self::Service
            {
                tower::util::BoxService::new(#new_ident.into_service(state))
            }

            fn message(&self) -> ::zebus::zebus_core::MessageTypeDescriptor
            {
                #new_ident.message()
            }

            fn queue(&self) -> Option<&'static str>
            {
                #queue
            }

            fn name(&self) -> &'static str
            {
                #new_ident.name()
            }

            fn subscription_mode(&self) -> ::zebus::zebus_core::SubscriptionMode {
                #mode
            }

            fn bindings(&self) -> Vec<Self::Binding> {
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

    let item_fn = parse_macro_input!(item as ItemFn);
    expand(item_fn, attrs).unwrap_or_else(|e| e.into_compile_error().into())
}
