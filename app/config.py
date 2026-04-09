import os
from dataclasses import dataclass


@dataclass
class Settings:
    app_name: str = os.getenv("APP_NAME", "Oracle AWR Report Analyzer")
    secret_key: str = os.getenv("SECRET_KEY", "change-me")
    llm_provider: str = os.getenv("LLM_PROVIDER", "ollama").lower()
    llm_model: str = os.getenv("LLM_MODEL", "llama3.1")

    # Ollama
    ollama_url: str = os.getenv("OLLAMA_URL", "http://localhost:11434")

    # AWS Bedrock
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    bedrock_model_id: str = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")

    # OCI GenAI
    oci_config_file: str = os.getenv("OCI_CONFIG_FILE", "/root/.oci/config")
    oci_profile: str = os.getenv("OCI_PROFILE", "DEFAULT")
    oci_compartment_id: str = os.getenv("OCI_COMPARTMENT_ID", "")
    oci_endpoint: str = os.getenv("OCI_GENAI_ENDPOINT", "")
    oci_model_id: str = os.getenv("OCI_MODEL_ID", "")


settings = Settings()
