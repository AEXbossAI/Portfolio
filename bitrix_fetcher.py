from typing import Dict, Any, List, Optional
import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta
from app.config import Config, load_client_config, MAX_CONCURRENT_CALLS
from app.pipeline import process_call_async  # Новый пайплайн!
import time

logger = logging.getLogger(__name__)

def get_bitrix_date_range(date_from: str, date_to: str, tz_offset_hours: int = 3):
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    date_from_utc = date_from_dt - timedelta(hours=tz_offset_hours)
    date_to_utc = date_to_dt - timedelta(hours=tz_offset_hours) + timedelta(hours=23, minutes=59, seconds=59)
    str_from = date_from_utc.strftime("%Y-%m-%d %H:%M:%S")
    str_to = date_to_utc.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[get_bitrix_date_range] output: str_from={str_from}, str_to={str_to}")
    return str_from, str_to

async def fetch_managers(company_id: str):
    client_config = load_client_config(company_id)
    webhook_url = client_config["BITRIX_WEBHOOK_URL"].rstrip("/")
    managers = {}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{webhook_url}/user.get", json={"filter[ACTIVE]": "true"}) as response:
                if response.status != 200:
                    logger.exception(f"Ошибка получения пользователей: {response.status}")
                    return managers
                data = await response.json()
            users = data.get("result", [])
            for user in users:
                user_id = user.get("ID", "")
                first_name = user.get("NAME", "")
                last_name = user.get("LAST_NAME", "")
                managers[user_id] = f"{first_name} {last_name}".strip()
        except Exception as e:
            logger.exception(f"Ошибка получения списка менеджеров: {e}")
    return managers

async def fetch_call_recordings(
    date_from: str,
    date_to: str,
    responsible_ids: list | None = None,
    call_type: str = "",
    min_duration: int | None = None,
    max_duration: int | None = None,
    process_call_async_func=process_call_async,
    max_concurrent_calls: Optional[int] = None,
    company_id: str = ""
) -> int:
    logger.info(f"[fetch_call_recordings] RAW INPUT: date_from={repr(date_from)}, date_to={repr(date_to)}")
    logger.info(
        f"[fetch_call_recordings] input: date_from={date_from}, date_to={date_to}, responsible_ids={responsible_ids}, call_type={call_type}, min_duration={min_duration}, max_duration={max_duration}")
    
    # Используем настройку из конфига, если не указано иное
    if max_concurrent_calls is None:
        max_concurrent_calls = MAX_CONCURRENT_CALLS
    
    logger.info(f"[fetch_call_recordings] max_concurrent_calls={max_concurrent_calls}")
    
    # Создаем семафор для ограничения количества одновременных задач
    semaphore = asyncio.Semaphore(max_concurrent_calls)
    
    client_config = load_client_config(company_id)
    webhook_url = client_config["BITRIX_WEBHOOK_URL"].rstrip("/")
    processed_count = 0
    start = 0
    managers = await fetch_managers(company_id)
    date_from_fmt, date_to_fmt = get_bitrix_date_range(date_from, date_to, tz_offset_hours=3)
    
    async def process_single_call(activity, session, managers, webhook_url, min_duration, max_duration, process_call_async_func, company_id):
        """Обрабатывает один звонок с ограничением через семафор"""
        async with semaphore:
            return await process_activity(activity, session, managers, webhook_url, min_duration, max_duration, process_call_async_func, company_id)
    
    async def process_activity(activity, session, managers, webhook_url, min_duration, max_duration, process_call_async_func, company_id):
        """Обрабатывает одну активность (звонок)"""
        call_id = activity.get('ID', '')
        start_time = activity.get('START_TIME', '')
        end_time = activity.get('END_TIME', '')
        logger.info(f"[call] call_id={call_id}, START_TIME={start_time}, END_TIME={end_time}")
        
        audio_data = None
        call_duration = None
        
        # 1. Пробуем через disk.file.get по id из FILES или STORAGE_ELEMENT_IDS
        files = activity.get('FILES', [])
        storage_type_id = activity.get('STORAGE_TYPE_ID', '')
        storage_element_ids = activity.get('STORAGE_ELEMENT_IDS', [])
        file_id = None
        if files:
            file_info = files[0]
            file_id = file_info.get('id')
        elif storage_type_id == "2" and storage_element_ids:
            file_id = storage_element_ids[0]
        if file_id:
            async with session.get(f"{webhook_url}/disk.file.get", params={"id": file_id}) as file_response:
                file_data = await file_response.json()
                download_url = file_data.get("result", {}).get("DOWNLOAD_URL")
                logger.info(f"[call] call_id={call_id} DOWNLOAD_URL={download_url}")
                if download_url:
                    async with session.get(download_url) as audio_response:
                        content_type = audio_response.headers.get("Content-Type", "")
                        logger.info(f"[call] call_id={call_id} Content-Type: {content_type}")
                        if content_type.startswith("audio/"):
                            audio_data = await audio_response.read()
                            logger.info(f"[call] call_id={call_id} audio_data_size={len(audio_data)} байт")
        
        # 2. Если не получилось — fallback на voximplant по call_id и ±1 день
        if not audio_data and start_time:
            from dateutil.parser import parse as parse_date
            start_dt = parse_date(start_time)
            call_datetime = start_dt.replace(tzinfo=None)
            search_start = call_datetime - timedelta(days=1)
            search_end = call_datetime + timedelta(days=1)
            vox_params = {
                "filter[>CALL_START_DATE]": search_start.strftime("%Y-%m-%d %H:%M:%S"),
                "filter[<CALL_END_DATE]": search_end.strftime("%Y-%m-%d %H:%M:%S"),
                "filter[CRM_ACTIVITY_ID]": call_id
            }
            logger.info(f"[call] call_id={call_id} voximplant params: {vox_params}")
            async with session.post(f"{webhook_url}/voximplant.statistic.get", json=vox_params) as vox_response:
                vox_data = await vox_response.json()
                vox_records = vox_data.get("result", [])
                logger.info(f"[call] call_id={call_id} voximplant records count: {len(vox_records)}")
                for record in vox_records:
                    record_file_id = record.get("RECORD_FILE_ID")
                    call_duration = record.get("CALL_DURATION")
                    if record_file_id:
                        async with session.get(f"{webhook_url}/disk.file.get", params={"id": record_file_id}) as file_response:
                            file_data = await file_response.json()
                            download_url = file_data.get("result", {}).get("DOWNLOAD_URL")
                            if download_url:
                                async with session.get(download_url) as audio_response:
                                    content_type = audio_response.headers.get("Content-Type", "")
                                    if content_type.startswith("audio/"):
                                        audio_data = await audio_response.read()
                                        logger.info(f"[call] call_id={call_id} audio_data_size={len(audio_data)} байт (voximplant)")
                                        break
                    if audio_data:
                        break
            if not audio_data:
                logger.error(f"[call] call_id={call_id} Не удалось получить аудиофайл для активности")
                return False
        # --- Фильтрация по длительности звонка ---
        # Если длительность не определена, пробуем вычислить по разности END_TIME и START_TIME
        if call_duration is None:
            try:
                from dateutil.parser import parse as parse_date
                start_dt = parse_date(start_time)
                end_dt = parse_date(end_time)
                call_duration = int((end_dt - start_dt).total_seconds())
            except Exception as e:
                logger.error(f"[call] call_id={call_id} Error calculating duration: {e}")
                call_duration = 0
        if call_duration is not None:
            try:
                call_duration = int(call_duration)
            except Exception as e:
                logger.error(f"[call] call_id={call_id} Error converting duration to int: {e}")
                call_duration = 0
            if min_duration is not None and call_duration < min_duration:
                logger.info(f"[call] call_id={call_id} пропущен: длительность {call_duration} < min_duration {min_duration}")
                return False
            if max_duration is not None and call_duration > max_duration:
                logger.info(f"[call] call_id={call_id} пропущен: длительность {call_duration} > max_duration {max_duration}")
                return False
        direction = activity.get("DIRECTION", "")
        call_type_str = "Входящий" if direction == "1" else "Исходящий"
        if "T" in start_time:
            call_date = start_time.split("T")[0]
            call_time = start_time.split("T")[1].split("+")[0]
        elif " " in start_time:
            call_date = start_time.split()[0]
            call_time = start_time.split()[1] if len(start_time.split()) > 1 else ""
        else:
            call_date = start_time
            call_time = ""
        entity_type = activity.get("OWNER_TYPE_ID", "")
        entity_id = activity.get("OWNER_ID", "")
        entity_link = get_entity_link(webhook_url, entity_type, entity_id)
        user_id = activity.get("RESPONSIBLE_ID", "")
        manager_name = managers.get(user_id, "Неизвестный менеджер")
        call_data = {
            "audio_data": audio_data,
            "call_id": call_id,
            "call_type": call_type_str,
            "call_duration": call_duration,
            "call_date": call_date,
            "call_time": call_time,
            "manager_name": manager_name,
            "entity_link": entity_link
        }
        try:
            success = await process_call_async_func(call_data, company_id)
            return success
        except Exception as e:
            logger.exception(f"Ошибка обработки звонка ID {call_id}: {e}")
            return False

    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                'start': start,
                'filter': {
                    '>=START_TIME': date_from_fmt,
                    '<=START_TIME': date_to_fmt,
                    'TYPE_ID': '2',
                },
                'select': [
                    'ID', 'OWNER_ID', 'OWNER_TYPE_ID', 'TYPE_ID', 'PROVIDER_ID', 'SUBJECT', 'START_TIME', 'END_TIME', 'DIRECTION', 'RESPONSIBLE_ID', 'STORAGE_TYPE_ID', 'STORAGE_ELEMENT_IDS', 'PROVIDER_DATA', 'FILES'
                ]
            }
            if responsible_ids:
                params['filter']['RESPONSIBLE_ID'] = responsible_ids
            if call_type == "incoming":
                params['filter']['DIRECTION'] = '1'
            elif call_type == "outgoing":
                params['filter']['DIRECTION'] = '2'
            logger.info(f"Bitrix params: {params}")
            try:
                async with session.post(f"{webhook_url}/crm.activity.list", json=params) as response:
                    if response.status != 200:
                        logger.exception(f"Ошибка Bitrix API: {response.status}")
                        return processed_count
                    data = await response.json()
                activities = data.get('result', [])
                logger.info(f"Получено {len(activities)} активностей на странице {start}")
                if not activities:
                    break
                
                # Создаем задачи для параллельной обработки
                tasks = []
                for activity in activities:
                    task = process_single_call(activity, session, managers, webhook_url, min_duration, max_duration, process_call_async_func, company_id)
                    tasks.append(task)
                
                # Запускаем все задачи параллельно и ждем результатов
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Подсчитываем успешные обработки
                for result in results:
                    if isinstance(result, Exception):
                        logger.exception(f"Ошибка в параллельной обработке: {result}")
                    elif result:
                        processed_count += 1
                
                start += len(activities)
                if len(activities) < 50:
                    break
            except Exception as e:
                logger.exception(f"Ошибка получения звонков: {e}")
                break
    
    logger.info(f"Обработано {processed_count} звонков")
    return processed_count

def get_entity_link(webhook_url: str, owner_type_id: str, owner_id: str) -> str:
    """
    Возвращает ссылку на сущность, связанную со звонком (лид, сделка, контакт).
    """
    try:
        portal_domain = webhook_url.split("/rest/")[0].replace("https://", "")
        if owner_type_id == "1":
            entity_link = f"https://{portal_domain}/crm/lead/details/{owner_id}/"
            logger.debug(f"Звонок привязан к лиду. Ссылка: {entity_link}")
            return entity_link
        elif owner_type_id == "2":
            entity_link = f"https://{portal_domain}/crm/deal/details/{owner_id}/"
            logger.debug(f"Звонок привязан к сделке. Ссылка: {entity_link}")
            return entity_link
        elif owner_type_id == "3":
            entity_link = f"https://{portal_domain}/crm/contact/details/{owner_id}/"
            logger.debug(f"Звонок привязан к контакту. Ссылка: {entity_link}")
            return entity_link
        else:
            logger.warning(f"Звонок не привязан к известной сущности: OWNER_TYPE_ID={owner_type_id}")
            return "Нет привязки"
    except Exception as e:
        logger.error(f"Ошибка при формировании ссылки на сущность: {e}")
        return "Ошибка"