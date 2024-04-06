from deduplication_configuration import ModelConf
import logging
import torch
import transformers
from transformers import AutoTokenizer
import re
import time
from huggingface_hub import login



class GenerateArticleSummaries:
    def __init__(self, config):
        logging.basicConfig(level=logging.DEBUG)

    # @staticmethod
    '''def load_models():
        #global tokenizer, pipeline
        deduplication_config = ModelConf()
        login(token=deduplication_config.huggingface_hub_token)

        tokenizer = AutoTokenizer.from_pretrained(deduplication_config.model_name,
                                                  token=deduplication_config.huggingface_hub_token)

        pipeline = transformers.pipeline(
            "text-generation",
            model=deduplication_config.model_name,
            torch_dtype=torch.float16,
            device_map={"": 0},
        )'''

    def generate_article_summaries(self, project_id, articles_list):
        logging.info("Generating articles summaries for project id: %s", project_id)
        logging.info("Number of articles to be processed is: %d", len(articles_list))

        deduplication_config = ModelConf()
        login(token=deduplication_config.huggingface_hub_token)
        start = time.time()
        tokenizer = AutoTokenizer.from_pretrained(deduplication_config.model_name,
                                                  token=deduplication_config.huggingface_hub_token)

        pipeline = transformers.pipeline(
            "text-generation",
            model=deduplication_config.model_name,
            torch_dtype=torch.float16,
            device_map={"": 0},
        )
        logging.debug("Model loading time is: %f sec", time.time() - start)
        start = time.time()

        article_count = 0
        for article in articles_list:
            logging.debug("Processing article number: %d", article_count + 1)
            max_length = 4000
            original_text = article['Content']
            modified_text = re.sub(r'\n', ' ', original_text)[:max_length]
            article.pop('Content')
            sequences = pipeline(
                f"""<s>[INST] <<SYS>>
                 You are an expert summarizer and you generate an extractive summary of the text provided by a user. Return your response as a paragraph. Summarize as briefly as possible.
                  <</SYS>>

                {modified_text}[/INST]""",
                do_sample=True,
                top_k=10,
                num_return_sequences=1,
                eos_token_id=tokenizer.eos_token_id,
                return_full_text=False,
                max_length=4000)

            article.update({'Summary': (sequences[0]['generated_text'])})
            torch.cuda.empty_cache()
            article_count += 1
            logging.info("Processed %s  articles", article_count)

        logging.debug("Total time taken to generate summaries: %f sec", time.time() - start)

        return articles_list
