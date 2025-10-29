# FABRIC MULTIMODAL AI SERVICES
# Get workload endpoints and access token
from synapse.ml.fabric.service_discovery import get_fabric_env_config
from synapse.ml.fabric.token_utils import TokenUtils
import json
import requests

fabric_env_config = get_fabric_env_config().fabric_env_config
auth_header = TokenUtils().get_openai_auth_header()

# OpenAI service endpoint for vision models
openai_base_host = fabric_env_config.ml_workload_endpoint + "cognitive/openai/openai/"
deployment_name = "gpt-4.1"  # or gpt-4.1
service_url = openai_base_host + f"deployments/{deployment_name}/chat/completions?api-version=2025-04-01-preview"

auth_headers = {
    "Authorization": auth_header,
    "Content-Type": "application/json"
}

def print_vision_response(messages, response):
    print("==========================================================================================")
    print("| OpenAI Vision Input    |")
    for msg in messages:
        if msg["role"] == "system":
            print("[System] ", msg["content"])
        elif msg["role"] == "user":
            if isinstance(msg["content"], list):
                for content_item in msg["content"]:
                    if content_item["type"] == "text":
                        print("Q: ", content_item["text"])
                    elif content_item["type"] == "image_url":
                        print("Image: [Image URL provided]")
            else:
                print("Q: ", msg["content"])
    print("------------------------------------------------------------------------------------------")
    print("| Response Status |", response.status_code)
    print("------------------------------------------------------------------------------------------")
    print("| OpenAI Output   |")
    if response.status_code == 200:
        print(response.json()["choices"][0]["message"]["content"])
    else:
        print(response.content.decode('utf-8'))
    print("==========================================================================================")

# Test payload with public image URL (this should work)
test_payload = {
    "messages": [
        {
            "role": "system", 
            "content": "You are an expert OCR assistant. Extract all text from images accurately while preserving formatting and structure."
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Please extract all text from this image. Maintain original formatting and structure."
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "https://assets-global.website-files.com/6253f6e60f27498e7d4a1e46/6271a55db22aea4fa0cce308_Consulting-Invoice-Template-1-G.jpeg",
                        "detail": "high"
                    }
                }
            ]
        }
    ],
    "max_tokens": 4000,
    "temperature": 0.1
}


response = requests.post(service_url, headers=auth_headers, json=test_payload)
print_vision_response(test_payload["messages"], response)
