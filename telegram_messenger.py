# app/messengers/telegram_messenger.py

import random
import asyncio
from telethon import events
from telethon.tl.types import PeerUser
from app.core.logger import logger
from app.core.telegram_client import client
from app.core.message_tracker import get_last_processed, set_last_processed, load_tracker
from app.core.dialog_service import DialogService
from app.core.admin_notifications import process_bot_reply
from app.core.threads import thread_manager
from app.core.config import config

dialog_service = DialogService()

def is_owner(user_id, config):
    return user_id == config.ADMIN_TELEGRAM_ID

def setup_telegram_handlers(config):
    @client.on(events.NewMessage)
    async def handle_message(event):
        logger.info(f"=== Получено новое сообщение от {event.message.sender_id} ===")
        user_id = event.message.sender_id
        chat_id = event.chat_id
        message = event.message.message

        bot_account = await client.get_me()

        if user_id == bot_account.id or is_owner(user_id, config):
            if dialog_service.is_disable_command(message):
                dialog_service.disable_chat(chat_id)
                logger.info(f"Бот отключен в чате {chat_id} владельцем")
                await event.delete()
                await client.send_message(chat_id, "Автоматические ответы отключены. Продолжаем общение.")
                return
            elif dialog_service.is_enable_command(message):
                dialog_service.enable_chat(chat_id)
                logger.info(f"Бот включен в чате {chat_id} владельцем")
                await event.delete()
                await client.send_message(chat_id, "Автоматические ответы включены.")
                return

        if dialog_service.is_disabled(chat_id) or dialog_service.is_disabled(user_id):
            return

        if hasattr(handle_message, 'processing') and user_id in handle_message.processing:
            return
        try:
            if not hasattr(handle_message, 'processing'):
                handle_message.processing = set()
            handle_message.processing.add(user_id)

            # --- Ключевой блок: определяем assistant_id для этого пользователя ---
            info = thread_manager.get_thread_info(user_id)
            if info:
                thread_id = info["thread_id"]
                assistant_id = info["assistant_id"]
            else:
                # Для новых диалогов — INBOUND_ASSISTANT_ID
                thread_id = await dialog_service.get_or_create_thread(user_id, assistant_id=config.INBOUND_ASSISTANT_ID)
                assistant_id = config.INBOUND_ASSISTANT_ID
                thread_manager.set_thread(user_id, thread_id, assistant_id=assistant_id)
            # ---------------------------------------------------------------------

            sender = await event.get_sender()
            username = getattr(sender, 'username', None)
            known_data = dialog_service.get_known_data(user_id)

            if event.message.voice:
                logger.info(f"Получено голосовое сообщение от {user_id}")
                user_message = await dialog_service.transcribe_voice(event.message.voice, client)
                if not user_message:
                    await client.send_message(user_id, "Извините, не удалось распознать голосовое сообщение.")
                    return

                logger.info(f"Распознанный текст: {user_message}")
                bot_reply = await dialog_service.get_assistant_response(thread_id, user_message, assistant_id=assistant_id)
                await process_bot_reply(bot_reply, user_id, username, known_data)
                set_last_processed(user_id, event.message.id)
            else:
                delay = random.randint(10, 30)
                combined_message, last_message_id = await dialog_service.collect_messages(
                    user_id,
                    event.message.id,
                    delay,
                    client
                )

                if dialog_service.is_stop_word(combined_message):
                    dialog_service.disable_chat(chat_id)
                    logger.info(f"Бот отключен в чате {chat_id} по стоп-слову пользователя")
                    await client.send_message(chat_id, "Автоматические ответы отключены.")
                    return

                logger.info(f"Обработка текстового сообщения от {user_id}: {combined_message}")
                bot_reply = await dialog_service.get_assistant_response(thread_id, combined_message, assistant_id=assistant_id)
                await process_bot_reply(bot_reply, user_id, username, known_data)
                set_last_processed(user_id, last_message_id)

        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {e}")
            await client.send_message(user_id, "Извините, произошла ошибка при обработке сообщения.")

        finally:
            if hasattr(handle_message, 'processing'):
                handle_message.processing.discard(user_id)

    # Аналогично можно вынести обработчик events.MessageEdited и другие функции

# Не забудь вызвать setup_telegram_handlers(config) при инициализации!