use std::collections::{BTreeMap, HashMap};

use vector_lib::{event::Event, partition::Partitioner};

use crate::{internal_events::TemplateRenderingError, template::Template};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChroniclePartitionKey {
    pub log_type: String,
    pub namespace: Option<String>,
    pub labels: Option<BTreeMap<String, String>>,
}

/// Partitions items based on the generated key for the given event.
pub struct ChroniclePartitioner {
    log_type: Template,
    fallback_log_type: Option<String>,
    namespace_template: Option<Template>,
    label_templates: Option<HashMap<String, Template>>,
}

impl ChroniclePartitioner {
    pub fn new(
        log_type: Template,
        fallback_log_type: Option<String>,
        namespace_template: Option<Template>,
        label_templates: Option<HashMap<String, Template>>,
    ) -> Self {
        Self {
            log_type,
            fallback_log_type,
            namespace_template,
            label_templates,
        }
    }
}

impl Partitioner for ChroniclePartitioner {
    type Item = Event;
    type Key = Option<ChroniclePartitionKey>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let log_type = self
            .log_type
            .render_string(item)
            .or_else(|error| {
                if let Some(fallback_log_type) = &self.fallback_log_type {
                    emit!(TemplateRenderingError {
                        error,
                        field: Some("log_type"),
                        drop_event: false,
                    });
                    Ok(fallback_log_type.clone())
                } else {
                    Err(emit!(TemplateRenderingError {
                        error,
                        field: Some("log_type"),
                        drop_event: true,
                    }))
                }
            })
            .ok()?;

        let namespace = self
            .namespace_template
            .as_ref()
            .map(|namespace| {
                namespace.render_string(item).map_err(|error| {
                    emit!(TemplateRenderingError {
                        error,
                        field: Some("namespace"),
                        drop_event: true,
                    });
                })
            })
            .transpose()
            .ok()?;
        let labels = self.label_templates.as_ref().map(|templates| {
            templates
                .iter()
                .filter_map(|(key, template)| {
                    match template.render_string(item) {
                        Ok(value) => Some((key.clone(), value)),
                        Err(error) => {
                            emit!(TemplateRenderingError {
                                error,
                                field: Some("labels"),
                                drop_event: false,
                            });
                            None
                        }
                    }
                })
                .collect::<BTreeMap<_, _>>()
        }).filter(|m| !m.is_empty());

        Some(ChroniclePartitionKey {
            log_type,
            namespace,
            labels,
        })
    }
}
