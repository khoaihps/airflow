import openai
import os
from typing import Optional

class OpenAISummarizer:
    def __init__(self):
        openai.api_key = os.getenv("OPENAI_API_KEY")

    def summarize(self, content: str) -> Optional[str]:
        try:
            prompt = (
                "Summarize below news within 4 sentences:\n\n"
                f"{content}"
            )
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"[OpenAI] Error: {e}")
            return None
