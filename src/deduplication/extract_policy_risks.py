from deduplication_configuration import ModelConf
import logging
import torch
import transformers
from transformers import AutoTokenizer
import re
import time
from huggingface_hub import login


class ExtractPolicyRisks:
    def __init__(self, config):
        logging.basicConfig(level=logging.DEBUG)

    def extract_policy_risks(self, policy_body_id, policies_list):
        logging.info("Extracting policy risks for policy body id: %s", policy_body_id)
        logging.info("Number of policies to be processed is: %d", len(policies_list))

        extraction_config = ModelConf()
        login(token=extraction_config.huggingface_hub_token)
        start = time.time()
        tokenizer = AutoTokenizer.from_pretrained(extraction_config.model_name,
                                                  token=extraction_config.huggingface_hub_token)

        pipeline = transformers.pipeline(
            "text-generation",
            model=extraction_config.model_name,
            torch_dtype=torch.float16,
            device_map={"": 0},
        )
        logging.debug("Model loading time is: %f sec", time.time() - start)
        start = time.time()

        policy_count = 0
        for policy in policies_list:
            logging.debug("Processing article number: %d", policy_count + 1)
            max_length = 4000
            original_text = policy['Content']
            modified_text = re.sub(r'\n', ' ', original_text)[:max_length]
            # article.pop('Content')
            sequences = pipeline(
                f"""<s>[INST] <<SYS>>
                 You are an expert government policy analyst, and for a given policy document you identify and extract mentioned risks in the policy.
                  <</SYS>>

                {modified_text}[/INST]""",
                do_sample=True,
                top_k=2,
                num_return_sequences=1,
                eos_token_id=tokenizer.eos_token_id,
                return_full_text=False,
                max_length=4000)

            policy.update({'PolicyRisks': sequences[0]['generated_text']})
            torch.cuda.empty_cache()
            policy_count += 1
            logging.info("Processed %s  policies", policy_count)

        logging.debug("Total time taken to extract risks is: %f sec", time.time() - start)

        return policies_list
