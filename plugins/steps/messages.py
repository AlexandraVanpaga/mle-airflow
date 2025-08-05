from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_default')
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    message = f"✅ DAG `{dag_id}` завершился успешно.\nRun ID: `{run_id}`"
    hook.send_message(chat_id='-4856685146', text=message)

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_default')
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    message = f"❌ DAG `{dag_id}` завершился с ошибкой.\nRun ID: `{run_id}`"
    hook.send_message(chat_id='-4856685146', text=message)


