import logging
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    logging.info("SUCCESS callback triggered")
    try:
        hook = TelegramHook(token='8351974589:AAGEnSiHQTpS98r1O2Xo_FG4aMp66Yqraao', chat_id='-4856685146')
        message = f"DAG {context['dag'].dag_id} успешно завершён! Run ID: {context['run_id']}"
        hook.send_message(text=message, chat_id='-4856685146')
    except Exception as e:
        logging.error(f"Ошибка при отправке SUCCESS сообщения в Telegram: {e}")

def send_telegram_failure_message(context):
    logging.info("FAILURE callback triggered")
    try:
        hook = TelegramHook(token='8351974589:AAGEnSiHQTpS98r1O2Xo_FG4aMp66Yqraao', chat_id='-4856685146')
        run_id = context['run_id']
        task_instance_key_str = context['task_instance_key_str']
        message = f'❌ Ошибка выполнения DAG!\nRun ID: {run_id}\nTask: {task_instance_key_str}'
        hook.send_message(text=message, chat_id='-4856685146')
    except Exception as e:
        logging.error(f"Ошибка при отправке FAILURE сообщения в Telegram: {e}")
