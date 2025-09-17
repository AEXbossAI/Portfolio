import logging
import json
import asyncio
from typing import Optional, Dict, Any
from openai import AsyncOpenAI
from app.config import load_client_config, Config

logger = logging.getLogger(__name__)

class OpenAIProcessor:
    def __init__(self, company_id):
        self.company_id = company_id
        client_config = load_client_config(company_id)
        self.client = AsyncOpenAI(api_key=Config.OPENAI_API_KEY)
        self.semaphore = asyncio.Semaphore(5)
        self.retries = 3
        self.retry_delay = 1
        self.assistant_id = client_config.get("OPENAI_ASSISTANT_ID")

    async def create_thread_with_message(self, content: str) -> Optional[str]:
        try:
            thread = await self.client.beta.threads.create()
            await self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=content
            )
            return thread.id
        except Exception as e:
            logger.exception(f"Ошибка создания треда: {e}")
            return None

    async def wait_for_run_completion(self, thread_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        max_attempts = 60
        attempt = 0
        while attempt < max_attempts:
            try:
                run = await self.client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run_id
                )
                if run.status == "completed":
                    messages = await self.client.beta.threads.messages.list(
                        thread_id=thread_id
                    )
                    for message in messages.data:
                        if message.role == "assistant":
                            try:
                                # Берем только text-блоки
                                text_blocks = [block for block in message.content if getattr(block, "type", None) == "text"]
                                response_text = None
                                for block in text_blocks:
                                    try:
                                        response_text = block.text.value.strip()
                                        break
                                    except Exception:
                                        continue
                                if not response_text:
                                    logger.warning("Нет text-блока в ответе ассистента или он пустой")
                                    return {}
                                return json.loads(response_text)
                            except (json.JSONDecodeError, AttributeError, IndexError) as e:
                                logger.exception(f"Ошибка парсинга ответа ассистента: {e}")
                                logger.warning(f"Сырой ответ ассистента: {response_text if response_text else 'NO_TEXT_BLOCK'}")
                                return {}
                elif run.status in ["failed", "cancelled", "expired"]:
                    logger.exception(f"Run failed with status: {run.status}")
                    return {}
                await asyncio.sleep(1)
                attempt += 1
            except Exception as e:
                logger.exception(f"Ошибка при проверке статуса: {e}")
                return {}
        logger.exception("Превышено время ожидания ответа от ассистента")
        return {}

    async def process_transcript_with_retry(self, transcript: str) -> Optional[Dict[str, Any]]:
        for attempt in range(self.retries):
            try:
                async with self.semaphore:
                    thread_id = await self.create_thread_with_message(transcript)
                    if not thread_id:
                        logger.exception("Не удалось создать тред")
                        continue
                    run = await self.client.beta.threads.runs.create(
                        thread_id=thread_id,
                        assistant_id=self.assistant_id
                    )
                    logger.debug(f"Создан run {run.id} для треда {thread_id}")
                    result = await self.wait_for_run_completion(thread_id, run.id)
                    if result is not None:
                        logger.debug(f"Получен результат: {result}")
                        return result
                    else:
                        logger.exception("Получен пустой результат от ассистента")
            except Exception as e:
                logger.exception(f"Ошибка обработки (попытка {attempt + 1}): {e}")
            if attempt < self.retries - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))
        logger.warning("Все попытки обработки транскрипции исчерпаны, возвращаем пустой результат")
        return {}

    async def process_batch(self, transcripts: list) -> list:
        tasks = [self.process_transcript_with_retry(transcript) for transcript in transcripts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.exception(f"Ошибка обработки транскрипции {i}: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)
        return processed_results