from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime, timedelta


# Функция для получения данных с Steam API
def get_steam_games():
    # Пример запроса к Steam Store API для получения популярных игр
    url = "https://store.steampowered.com/api/featuredcategories"
    response = requests.get(url)
    data = response.json()

    # Здесь можно обработать и фильтровать данные (например, топ-5 игр)
    top_games = []
    for category in data['specials']['items'][:5]:  # Берём только первые 5 игр
        game_info = {
            'name': category['name'],
            'discount': category['discount_percent'],
            'price': category['final_price'],
            'url': category.get('store_url', 'URL не найден')
        }
        top_games.append(game_info)

    return top_games


# Функция для отправки данных в Telegram
def send_to_telegram(message: str):
    token = 'your_telegram_bot_token'  # Замените на токен вашего бота
    chat_id = 'your_chat_id'  # Замените на ваш chat_id
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url)


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
)

# Операторы
get_games_task = PythonOperator(
    task_id='get_steam_games',
    python_callable=get_steam_games,
    dag=dag,
)


def format_message(games):
    message = "Топ 5 игр на Steam:\n"
    for i, game in enumerate(games, start=1):
        message += f"{i}. {game['name']} (Скидка: {game['discount']}%)\nЦена: {game['price']}\nСсылка: {game['url']}\n\n"
    return message


send_telegram_task = PythonOperator(
    task_id='send_to_telegram',
    python_callable=send_to_telegram,
    op_args=[format_message(get_steam_games())],
    dag=dag,
)

# Задачи
get_games_task >> send_telegram_task
