from openai import OpenAI
import os
from typing import Optional

class OpenAISummarizer:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY")
        )

    def summarize(self, content: str) -> Optional[str]:
        try:
            prompt = (
                "Summarize below news within 4 sentences:\n\n"
                f"{content}"
            )
            response = self.client.responses.create(
                model="gpt-3.5-turbo",
                instructions="You are an assistant capable of summarizing financial news.",
                input=prompt,
            )
            return response.output_text
        except Exception as e:
            print(f"[OpenAI] Error: {e}")
            return None
