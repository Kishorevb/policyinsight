from dataclasses import dataclass


@dataclass(kw_only=True)
class ModelConf:
    model_name = "meta-llama/Llama-2-7b-chat-hf"
    huggingface_hub_token = 'xxxxxxx'
    embedding_model = "https://tfhub.dev/google/universal-sentence-encoder-multilingual-large/3"

