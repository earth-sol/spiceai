use crate::get_params_with_secrets;
use crate::parameters::{ParameterSpec, Parameters};
use crate::secrets::Secrets;
use secrecy::SecretString;
use serde_json::Value;
use snafu::prelude::*;
use spicepod::component::model::Model as SpicepodModel;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid model type '{model_type}' specified"))]
    UnsupportedModelSource { model_type: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ModelParams {
    pub(crate) parameters: Parameters,
    pub(crate) model: SpicepodModel,
    // Temporary field to store extracted secret params
    // TODO: remove the filed when all models are created using ModelParams
    pub(crate) secret_params: HashMap<String, SecretString>,
}

pub struct ModelParamsBuilder {
    model: Arc<SpicepodModel>,
}

impl ModelParamsBuilder {
    #[must_use]
    pub fn new(model: Arc<SpicepodModel>) -> Self {
        Self { model }
    }

    pub async fn build(
        &self,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<ModelParams, Box<dyn std::error::Error + Send + Sync>> {
        let source = self.model.from.to_string();

        // Convert params to HashMap<String, String>
        // TODO: Have downstream code using model parameters to accept `Hashmap<String, Value>`.
        // This will require handling secrets with `Value` type.
        let string_params: HashMap<String, String> = self
            .model
            .params
            .clone()
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                match v.as_str() {
                    Some(s) => (k, s.to_string()),
                    None => (k, v.to_string()),
                }
            })
            .collect::<HashMap<_, _>>();
        // Get parameter specs based on source
        let (params, prefix, parameters) = match source.as_str() {
            "openai" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "openai",
                ModelParameterSpecs::openai(),
            ),
            "azure_openai" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "azure",
                ModelParameterSpecs::azure_openai(),
            ),
            "anthropic" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "anthropic",
                ModelParameterSpecs::anthropic(),
            ),
            "huggingface" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "huggingface",
                ModelParameterSpecs::huggingface(),
            ),
            "perplexity" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "perplexity",
                ModelParameterSpecs::perplexity(),
            ),
            "local" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "file",
                ModelParameterSpecs::local(),
            ),
            "xai" => (
                get_params_with_secrets(Arc::clone(&secrets), &string_params).await,
                "xai",
                ModelParameterSpecs::xai(),
            ),
            _ => {
                return Err(Error::UnsupportedModelSource {
                    model_type: source.clone(),
                }
                .into())
            }
        };

        let secret_params = params.clone();

        let parameters = Parameters::try_new(
            &format!("model {}", self.model.name),
            params.into_iter().collect(),
            prefix,
            secrets,
            parameters,
        )
        .await?;

        Ok(ModelParams {
            parameters,
            model: (*self.model).clone(),
            secret_params,
        })
    }
}

pub struct ModelParameterSpecs;

impl ModelParameterSpecs {
    pub fn openai() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![
                ParameterSpec::connector("endpoint")
                    .description("The OpenAI API base endpoint")
                    .default("https://api.openai.com/v1"),
                ParameterSpec::connector("openai_api_key")
                    .description("OpenAI API key")
                    .required()
                    .secret(),
                ParameterSpec::connector("openai_org_id")
                    .description("The OpenAI organization ID")
                    .required()
                    .secret(),
                ParameterSpec::connector("openai_project_id")
                    .description("The OpenAI project ID")
                    .required()
                    .secret(),
                ParameterSpec::connector("model").description("The OpenAI model to use"),
                ParameterSpec::connector("openai_temperature")
                    .description("Sampling temperature (0.0-2.0)"),
                ParameterSpec::connector("max_tokens").description("Maximum tokens in response"),
                ParameterSpec::connector("response_format")
                    .description("Response format specification"),
            ]
        });
        &PARAMETERS
    }

    pub fn azure_openai() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![
                ParameterSpec::connector("azure_api_key")
                    .description("Azure OpenAI API key")
                    .required()
                    .secret(),
                ParameterSpec::connector("endpoint")
                    .description("Azure OpenAI endpoint")
                    .required()
                    .secret(),
                ParameterSpec::connector("azure_deployment_name")
                    .description("Azure OpenAI deployment name")
                    .required()
                    .secret(),
                ParameterSpec::connector("azure_entra_token")
                    .description("Azure Entra token")
                    .required()
                    .secret(),
                ParameterSpec::connector("azure_api_version")
                    .description("API version")
                    .required()
                    .secret(),
                ParameterSpec::connector("azure_project_id").description("Project ID"),
            ]
        });
        &PARAMETERS
    }

    pub fn anthropic() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![
                ParameterSpec::connector("anthropic_api_key")
                    .description("The Anthropic API key.")
                    .required()
                    .secret(),
                ParameterSpec::connector("endpoint")
                    .description("The Anthropic API base endpoint.")
                    .required()
                    .default("https://api.anthropic.com/v1"),
                ParameterSpec::connector("anthropic_auth_token")
                    .description("The Anthropic API base endpoint.")
                    .required()
                    .secret(),
            ]
        });
        &PARAMETERS
    }

    pub fn huggingface() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![
                ParameterSpec::connector("hf_token")
                    .description("The Huggingface access token.")
                    .required()
                    .secret(),
                ParameterSpec::connector("model_type")
                    .description("The architecture to load the model.")
                    .required()
                    .secret(),
                ParameterSpec::connector("tools")
                    .description("Which [tools] should be made available to the model. Set to auto to use all available tools.")
                    .default("auto"),
                ParameterSpec::connector("system_prompt")
                    .description("An additional system prompt used for all chat completions to this model.")        
            ]
        });
        &PARAMETERS
    }

    pub fn perplexity() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![ParameterSpec::connector("perplexity_auth_token")
                .description("The Perplexity API authentication token.")
                .required()
                .secret()]
        });
        &PARAMETERS
    }

    pub fn local() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![
            ParameterSpec::connector("model_type")
                .description("The architecture to load the model")
                .required(),
            ParameterSpec::connector("tools")
                .description("Which tools should be made available to the model. Set to auto to use all available tools.")
                .default("auto"),
            ParameterSpec::connector("system_prompt")
                .description("An additional system prompt used for all chat completions to this model."),
            ParameterSpec::connector("chat_template")
                .description("Customizes the transformation of OpenAI chat messages into a character stream for the model"),
        ]
        });
        &PARAMETERS
    }

    pub fn xai() -> &'static [ParameterSpec] {
        static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
            vec![ParameterSpec::connector("xai_api_key")
                .description("The xAI API key.")
                .required()
                .secret()]
        });

        &PARAMETERS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::Secrets;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn test_openai_parameter_specs() {
        let specs = ModelParameterSpecs::openai();

        // Verify required parameters
        let api_key = specs.iter().find(|s| s.name == "openai_api_key").unwrap();
        assert!(api_key.required);
        assert!(api_key.secret);

        // Verify optional parameters
        let temperature = specs
            .iter()
            .find(|s| s.name == "openai_temperature")
            .unwrap();
        assert!(!temperature.required);

        // Verify default values
        let endpoint = specs.iter().find(|s| s.name == "endpoint").unwrap();
        assert_eq!(endpoint.default.unwrap(), "https://api.openai.com/v1");
    }

    #[test]
    fn test_azure_openai_parameter_specs() {
        let specs = ModelParameterSpecs::azure_openai();

        // Verify required parameters
        for param in [
            "azure_api_key",
            "endpoint",
            "azure_deployment_name",
            "azure_entra_token",
            "azure_api_version",
        ] {
            let spec = specs.iter().find(|s| s.name == param).unwrap();
            assert!(spec.required, "Parameter {} should be required", param);
        }

        // Verify secret parameters
        for param in ["azure_api_key", "azure_entra_token"] {
            let spec = specs.iter().find(|s| s.name == param).unwrap();
            assert!(spec.secret, "Parameter {} should be secret", param);
        }
    }

    #[test]
    fn test_anthropic_parameter_specs() {
        let specs = ModelParameterSpecs::anthropic();

        // Verify API key
        let api_key = specs
            .iter()
            .find(|s| s.name == "anthropic_api_key")
            .unwrap();
        assert!(api_key.required);
        assert!(api_key.secret);

        // Verify endpoint default
        let endpoint = specs.iter().find(|s| s.name == "endpoint").unwrap();
        assert_eq!(endpoint.default.unwrap(), "https://api.anthropic.com/v1");
    }

    #[tokio::test]
    async fn test_model_params_builder() {
        let secrets = Arc::new(RwLock::new(Secrets::default()));

        // Test OpenAI model params
        let mut params = HashMap::new();
        params.insert(
            "openai_api_key".to_string(),
            Value::String("test-key".to_string()),
        );
        params.insert(
            "openai_org_id".to_string(),
            Value::String("test-org-id".to_string()),
        );
        params.insert(
            "openai_project_id".to_string(),
            Value::String("test-project-id".to_string()),
        );
        params.insert("model".to_string(), Value::String("gpt-4".to_string()));

        let model = SpicepodModel {
            name: "test-model".to_string(),
            from: "openai".to_string(), // Use 'from' instead of 'source'
            params: params,             // Convert HashMap to Value
            ..Default::default()        // Fill other required fields with defaults
        };

        let builder = ModelParamsBuilder::new(Arc::new(model));
        let result = builder.build(secrets.clone()).await;
        assert!(result.is_ok());
        // Test invalid source
        let mut params = HashMap::new();
        params.insert("api_key".to_string(), Value::String("test-key".to_string()));
        params.insert("model".to_string(), Value::String("gpt-4".to_string()));

        let invalid_model = SpicepodModel {
            name: "invalid-model".to_string(),
            from: "invalid-source".to_string(),
            params: params,
            ..Default::default()
        };

        let builder = ModelParamsBuilder::new(Arc::new(invalid_model));
        let result = builder.build(secrets.clone()).await;
        assert!(matches!(
            result.unwrap_err().downcast_ref::<Error>(),
            Some(Error::UnsupportedModelSource { model_type }) if model_type == "invalid-source"
        ));
    }

    #[test]
    fn test_huggingface_parameter_specs() {
        let specs = ModelParameterSpecs::huggingface();

        // Verify required parameters
        let token = specs.iter().find(|s| s.name == "hf_token").unwrap();
        assert!(token.required);
        assert!(token.secret);

        // Verify default values
        let tools = specs.iter().find(|s| s.name == "tools").unwrap();
        assert_eq!(tools.default.unwrap(), "auto");
    }

    #[test]
    fn test_local_parameter_specs() {
        let specs = ModelParameterSpecs::local();

        // Verify required parameters
        let model_type = specs.iter().find(|s| s.name == "model_type").unwrap();
        assert!(model_type.required);

        // Verify optional parameters
        let tools = specs.iter().find(|s| s.name == "tools").unwrap();
        assert_eq!(tools.default.unwrap(), "auto");
    }

    #[test]
    fn test_xai_parameter_specs() {
        let specs = ModelParameterSpecs::xai();

        // Verify API key
        let api_key = specs.iter().find(|s| s.name == "xai_api_key").unwrap();
        assert!(api_key.required);
        assert!(api_key.secret);
    }
}
