from airflow.providers.telegram.hooks.telegram import TelegramHook

def test_send_telegram():
    token = '8351974589:AAGEnSiHQTpS98r1O2Xo_FG4aMp66Yqraao'  # твой токен
    chat_id = '-4856685146'  # твой chat_id

    hook = TelegramHook(token=token, chat_id=chat_id)
    hook.send_message({
        'chat_id': chat_id,
        'text': 'Тестовое сообщение из локального скрипта'
    })
    print("Сообщение отправлено!")

if __name__ == "__main__":
    test_send_telegram()
