use std::collections::{hash_map::Entry, HashMap};

use proc_macro::TokenStream;
use quote::quote;
use syn::{spanned::Spanned, Data, DataStruct, DeriveInput, Fields};

use crate::attrs::find_attrs;

use super::{attrs::ZebusStructAttrs, field::Field};

/// A field with a `routing_position` attribute
struct RoutingField {
    /// Underlying syn representation fo the field
    field: syn::Field,
    /// Field declaration index
    index: usize,
    /// `routing_positition` attribute of #[zebus] attribute
    routing_position: usize,
}

impl RoutingField {
    fn ident(&self) -> Option<&syn::Ident> {
        self.field.ident.as_ref()
    }
}

fn routing_fields(
    ident: &syn::Ident,
    attrs: &ZebusStructAttrs,
    fields: &[Field],
) -> syn::Result<Vec<RoutingField>> {
    // Filter routing fields
    let mut routing_fields = fields
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| match field {
            Field::Normal(..) => None,
            Field::Routed { field, position } => Some(RoutingField {
                field: field.clone(),
                index: idx,
                routing_position: *position,
            }),
        })
        .collect::<Vec<_>>();

    // Make sure the routable attribute is applied when appropriate
    if attrs.routable.unwrap_or(false) {
        if routing_fields.is_empty() {
            return Err(syn::Error::new_spanned(
                ident,
                "a routable message must have at least one field with a `routing_position` attribute",
            ));
        }
    } else if let Some(field) = routing_fields.first() {
        return Err(syn::Error::new_spanned(
            &field.field,
            "a non-routable message must not have any field with a `routing_position` attribute",
        ));
    }

    // Sanity check that fields do not have duplicated routing positions
    let mut unique = HashMap::new();
    for field in &routing_fields {
        let routing_position = field.routing_position;

        match unique.entry(routing_position) {
            Entry::Occupied(e) => {
                let orig_field: &&RoutingField = e.get();
                let orig_field_name = orig_field.ident().expect("field should have an ident");

                return Err(
                    syn::Error::new_spanned(
                        &field.field,
                        format!("duplicated field with routing_position. already defined by `{orig_field_name}`"),
                    )
                );
            }
            Entry::Vacant(e) => e.insert(field),
        };
    }

    // Sort routing fields by their routing position
    routing_fields.sort_by(|f1, f2| f1.routing_position.cmp(&f2.routing_position));
    Ok(routing_fields)
}

// Generate implementation of `MessageBinding` trait
fn message_binding(
    ident: &syn::Ident,
    routing_fields: &[RoutingField],
) -> proc_macro2::TokenStream {
    // Generate the type that will be associated with the `Binding` type of `BindingExpression` trait
    let (message_binding_struct_expanded, name) = {
        let name = syn::Ident::new(&format!("{ident}Binding"), ident.span());
        let fields = routing_fields
            .iter()
            .map(|routing_field| {
                let field = &routing_field.field;
                let ident = routing_field.ident().expect("field should have an ident");
                let ty = &field.ty;

                quote! {
                    pub #ident: ::zebus::zebus_core::Binding<#ty>,
                }
            })
            .collect::<Vec<_>>();

        let expanded = if fields.is_empty() {
            quote! {
                #[derive(Default)]
                pub struct #name;
            }
        } else {
            quote! {
                #[derive(Default)]
                pub struct #name {
                    #( #fields )*
                }
            }
        };

        (expanded, name)
    };

    // Generate implementation of `MessageBinding` trait
    let message_binding_impl_expanded = {
        let bind_expanded = {
            let fragments = routing_fields
                .iter()
                .map(|routing_field| {
                    let ident = routing_field.ident().expect("field should have an ident");

                    quote! {
                        binding.#ident.bind(),
                    }
                })
                .collect::<Vec<_>>();

            if fragments.is_empty() {
                quote! {
                    ::zebus::zebus_core::BindingKey {
                        fragments: None
                    }
                }
            } else {
                quote! {
                    ::zebus::zebus_core::BindingKey {
                        fragments: Some(vec![#( #fragments )*])
                    }
                }
            }
        };

        quote! {
            impl ::zebus::zebus_core::BindingExpression for #ident {
                type Binding = #name;

                fn bind(binding: Self::Binding) -> ::zebus::zebus_core::BindingKey {
                    #bind_expanded
                }
            }
        }
    };

    quote! {
        #message_binding_struct_expanded

        #message_binding_impl_expanded
    }
}

// Generate implementation of `Message` trait
fn message_impl(
    ident: &syn::Ident,
    attrs: ZebusStructAttrs,
    routing_fields: &[RoutingField],
    kind: proc_macro2::TokenStream,
    derive: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let namespace = attrs.namespace.ok_or(syn::Error::new(
        attrs.span.unwrap_or(ident.span()),
        "missing required attribute `namespace`",
    ))?;
    let name = ident.to_string();
    let full_name = format!("{namespace}.{name}");

    let routing_fields_expanded = routing_fields.iter().map(|routing_field| {
        let index = routing_field.index;
        let routing_position = routing_field.routing_position;

        quote! {
            ::zebus::zebus_core::RoutingField {
                index: #index,
                routing_position: #routing_position
            }
        }
    });

    let get_binding = if routing_fields.is_empty() {
        quote! { ::zebus::zebus_core::BindingKey::default() }
    } else {
        let parts = routing_fields.iter().map(|routing_field| {
            let ident = routing_field.ident().expect("field should have an ident");
            quote! {
                self.#ident.to_string(),
            }
        });

        quote! {
            vec![#( #parts )*].into()
        }
    };

    let infrastructure = attrs.infrastructure.unwrap_or(false);
    let transient = attrs.transient.unwrap_or(false);

    let flags = match (infrastructure, transient) {
        (false, false) => quote! { ::zebus::zebus_core::MessageFlags::default() },
        (false, true) => quote! { ::zebus::zebus_core::MessageFlags::TRANSIENT },
        (true, false) => quote! { ::zebus::zebus_core::MessageFlags::INFRASTRUCTURE },
        (true, true) => {
            quote! { ::zebus::zebus_core::MessageFlags::INFRASTRUCTURE | ::zebus::MessageFlags::TRANSIENT }
        }
    };

    Ok(quote! {
        impl ::zebus::zebus_core::MessageDescriptor for #ident {
            fn kind() -> ::zebus::zebus_core::MessageKind {
                #kind
            }

            fn flags() -> ::zebus::zebus_core::MessageFlags {
                #flags
            }

            fn name() -> &'static str {
                #full_name
            }

            fn routing() -> &'static [::zebus::zebus_core::RoutingField] {
                &[#( #routing_fields_expanded, )*]
            }
        }

        impl ::zebus::zebus_core::Message for #ident {
            fn kind(&self) -> ::zebus::zebus_core::MessageKind {
                #kind
            }

            fn flags(&self) -> ::zebus::zebus_core::MessageFlags {
                #flags
            }

            fn name(&self) -> &'static str {
                #full_name
            }

            fn get_binding(&self) -> ::zebus::zebus_core::BindingKey {
                #get_binding
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        impl #derive for #ident {}
    })
}

fn message(
    input: TokenStream,
    kind: proc_macro2::TokenStream,
    derive: proc_macro2::TokenStream,
) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse(input)?;
    let span = input.span();
    let ident = &input.ident;

    let fields = match input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => Ok(fields.named.into_iter().collect::<Vec<_>>()),
        Data::Struct(..) => Err(syn::Error::new(
            span,
            "Command can not be derived for a struct with unnamed fields",
        )),
        Data::Enum(..) => Err(syn::Error::new(
            span,
            "Command can not be derived for an enum",
        )),
        Data::Union(..) => Err(syn::Error::new(
            span,
            "Command can not be derived for a union",
        )),
    }?;

    let root_attrs: ZebusStructAttrs = find_attrs(&input.attrs[..], "zebus")?;

    let fields = fields
        .into_iter()
        .map(Field::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let routing_fields = routing_fields(ident, &root_attrs, &fields[..])?;

    let message_impl_expanded = message_impl(ident, root_attrs, &routing_fields[..], kind, derive)?;
    let message_binding_expanded = message_binding(ident, &routing_fields[..]);

    let expanded = quote! {
        #message_impl_expanded

        #message_binding_expanded
    };

    Ok(expanded.into())
}

pub(crate) fn command(input: TokenStream) -> syn::Result<TokenStream> {
    message(
        input,
        quote! { ::zebus::zebus_core::MessageKind::Command },
        quote! { ::zebus::zebus_core::Command },
    )
}

pub(crate) fn event(input: TokenStream) -> syn::Result<TokenStream> {
    message(
        input,
        quote! { ::zebus::zebus_core::MessageKind::Event },
        quote! { ::zebus::zebus_core::Event },
    )
}
