from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging
from datetime import datetime, timedelta


# Функция для получения данных с Steam API
def get_steam_games():
    logging.info("Запуск получения данных с Steam API")
    url = "https://store.steampowered.com/api/featuredcategories"
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Ошибка при запросе к Steam API. Статус: {response.status_code}")
        return []

    try:
        data = response.json()
        top_games = []
        for category in data['specials']['items'][:5]:
            game_info = {
                'name': category['name'],
                'discount': category['discount_percent'],
                'price': category['final_price'],
                'url': category.get('store_url', 'URL не найден')
            }
            top_games.append(game_info)
        logging.info(f"Топ 5 игр: {top_games}")
        return top_games
    except KeyError as e:
        logging.error(f"Ошибка в данных Steam API: отсутствует ключ {e}")
        return []


# Функция для отправки данных в Telegram
def send_to_telegram(message: str):
    logging.info(f"Отправка сообщения в Telegram: {message}")
    token = 'your_telegram_bot_token'  # Замените на токен вашего бота
    chat_id = 'your_chat_id'  # Замените на ваш chat_id
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
    response = requests.get(url)
    if response.status_code == 200:
        logging.info("Сообщение отправлено успешно.")
    else:
        logging.error(f"Ошибка отправки сообщения в Telegram. Статус: {response.status_code}")


# Форматирование сообщения
def format_message(games):
    message = "Топ 5 игр на Steam:\n"
    for i, game in enumerate(games, start=1):
        message += f"{i}. {game['name']} (Скидка: {game['discount']}%)\nЦена: {game['price']}\nСсылка: {game['url']}\n\n"
    return message


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'steam_parser_dag',
    default_args=default_args,
    description='Парсер Steam API для получения данных о играх',
    schedule_interval=timedelta(days=1),  # Запуск каждый день
    catchup=False,  # Запуск только с текущего дня
)

# Операторы
get_games_task = PythonOperator(
    task_id='get_steam_games',
    python_callable=get_steam_games,
    dag=dag,
)

send_telegram_task = PythonOperator(
    task_id='send_to_telegram',
    python_callable=send_to_telegram,
    op_args=[format_message],
    op_kwargs={'games': get_steam_games()},
    dag=dag,
)

# Задачи
get_games_task >> send_telegram_task
