import json
from abc import ABC, abstractmethod

import boto3
import requests

from app.config import settings


class BaseLLMProvider(ABC):
    @abstractmethod
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        raise NotImplementedError


class OllamaProvider(BaseLLMProvider):
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        url = f"{settings.ollama_url.rstrip('/')}/api/chat"
        payload = {
            "model": settings.llm_model,
            "format": "json",
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        response = requests.post(url, json=payload, timeout=180)
        response.raise_for_status()
        data = response.json()
        return data.get("message", {}).get("content", "{}")


class BedrockProvider(BaseLLMProvider):
    def __init__(self) -> None:
        self.client = boto3.client("bedrock-runtime", region_name=settings.aws_region)

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        model_id = settings.bedrock_model_id
        if "anthropic" in model_id:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1800,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            resp = self.client.invoke_model(modelId=model_id, body=json.dumps(body))
            result = json.loads(resp["body"].read())
            text_items = result.get("content", [])
            return text_items[0].get("text", "{}") if text_items else "{}"

        body = {
            "inputText": f"{system_prompt}\n\n{user_prompt}",
            "textGenerationConfig": {"maxTokenCount": 1800, "temperature": 0.2},
        }
        resp = self.client.invoke_model(modelId=model_id, body=json.dumps(body))
        result = json.loads(resp["body"].read())
        return result.get("results", [{}])[0].get("outputText", "{}")


class OCIProvider(BaseLLMProvider):
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        import oci

        config = oci.config.from_file(settings.oci_config_file, settings.oci_profile)
        endpoint = settings.oci_endpoint or config.get("region")
        client = oci.generative_ai_inference.GenerativeAiInferenceClient(
            config=config,
            service_endpoint=settings.oci_endpoint,
            retry_strategy=oci.retry.NoneRetryStrategy(),
            timeout=(10, 180),
        )

        text_content = oci.generative_ai_inference.models.TextContent(text=f"{system_prompt}\n\n{user_prompt}")
        message = oci.generative_ai_inference.models.Message(role="USER", content=[text_content])
        chat_details = oci.generative_ai_inference.models.GenericChatRequest(
            api_format=oci.generative_ai_inference.models.BaseChatRequest.API_FORMAT_GENERIC,
            messages=[message],
            max_tokens=1800,
            temperature=0.2,
        )
        details = oci.generative_ai_inference.models.ChatDetails(
            compartment_id=settings.oci_compartment_id,
            serving_mode=oci.generative_ai_inference.models.OnDemandServingMode(model_id=settings.oci_model_id),
            chat_request=chat_details,
        )
        response = client.chat(details)
        choices = response.data.chat_response.choices
        if not choices:
            return "{}"
        return choices[0].message.content[0].text


def provider_factory() -> BaseLLMProvider:
    provider = settings.llm_provider
    if provider == "bedrock":
        return BedrockProvider()
    if provider == "oci":
        return OCIProvider()
    return OllamaProvider()
