"""
DAG для проверки отправки вакансий с hh.ru.
Запускается каждые 20 минут, но отправляет максимум 3 вакансии в неделю.
"""

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta, date
import requests
import json

# Функция для проверки, можно ли отправлять новую вакансию.
# Если за текущую неделю отправлено меньше 3 вакансий, возвращает имя ветки для отправки,
# иначе – ветку пропуска.
def should_send():
    # Определяем текущую дату, год и номер недели
    today = date.today()
    current_year = today.year
    current_week = today.isocalendar()[1]  # номер недели согласно ISO

    var_key = "vacancies_sent_week"
    # Пробуем получить данные из переменной Airflow
    try:
        data = Variable.get(var_key, deserialize_json=True)
    except KeyError:
        # Если переменной ещё нет, создаём её с нулевым счётчиком
        data = {"year": current_year, "week": current_week, "count": 0}

    # Если данные относятся не к текущему году/неделе – сбрасываем счётчик
    if data.get("year") != current_year or data.get("week") != current_week:
        data = {"year": current_year, "week": current_week, "count": 0}
        Variable.set(var_key, json.dumps(data))

    # Если за текущую неделю отправлено меньше 3 вакансий, выбираем ветку отправки
    if data.get("count", 0) < 3:
        return "fetch_and_send"
    else:
        # Иначе – выбираем ветку, которая ничего не делает
        return "skip_email"

# Функция для получения вакансии с hh.ru и подготовки данных для письма.
def fetch_and_send(**kwargs):
    # Адрес API hh.ru и параметры запроса
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "Data engineer",  # поисковый запрос
        "area": 1,                # код региона (можно изменить)
        "per_page": 20,           # число вакансий на странице
    }

    # Отправляем GET-запрос к API
    response = requests.get(url, params=params)
    response.raise_for_status()  # Если произошла ошибка, генерируется исключение
    data = response.json()
    vacancies = data.get("items", [])

    # Если вакансий нет, формируем сообщение об отсутствии новых вакансий
    if not vacancies:
        vacancy_html = "<p>Новых вакансий не найдено.</p>"
    else:
        # Для простоты выбираем первую вакансию из списка
        vacancy = vacancies[0]
        name = vacancy.get("name")
        employer = vacancy.get("employer", {}).get("name", "Неизвестно")
        link = vacancy.get("alternate_url")
        # Форматируем данные в HTML
        vacancy_html = (
            f"<h3>Новая вакансия Data engineer:</h3>"
            f"<p><b>{name}</b><br>"
            f"Компания: {employer}<br>"
            f"<a href='{link}'>Подробнее</a></p>"
        )

    # Обновляем переменную, увеличивая счётчик отправленных вакансий на 1
    var_key = "vacancies_sent_week"
    data_var = Variable.get(var_key, deserialize_json=True)
    count = data_var.get("count", 0)
    data_var["count"] = count + 1
    Variable.set(var_key, json.dumps(data_var))

    # Сохраняем отформатированный HTML через XCom для использования в EmailOperator
    kwargs["ti"].xcom_push(key="vacancy_html", value=vacancy_html)

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Указываем дату начала (измените по необходимости)
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаём DAG, который будет запускаться каждые 20 минут
with DAG(
    'hh_vacancies_test_dag_1',
    default_args=default_args,
    schedule_interval="*/20 * * * *",  # каждые 20 минут
    catchup=False,
    description="DAG для тестирования отправки вакансий (максимум 3 за неделю)"
) as dag:

    # Задача-ветвление: проверяет, можно ли отправлять вакансию
    check_send = BranchPythonOperator(
        task_id='should_send',
        python_callable=should_send,
    )

    # Задача для получения вакансии и подготовки письма, если лимит не превышен
    fetch_and_send_task = PythonOperator(
        task_id='fetch_and_send',
        python_callable=fetch_and_send,
        provide_context=True,
    )

    # Задача отправки email с полученной вакансией
    send_email = EmailOperator(
        task_id='send_email',
        to='alexeybelozerov93@gmail.com',  # адрес получателя
        subject='Новая вакансия Data engineer',
        # Получаем HTML-контент, который сохранили в XCom в задаче fetch_and_send
        html_content="{{ ti.xcom_pull(task_ids='fetch_and_send', key='vacancy_html') }}",
    )

    # Задача-заглушка для случая, когда лимит вакансий уже достигнут
    skip_email = DummyOperator(
        task_id='skip_email'
    )

    # Определяем последовательность выполнения:
    # Сначала проверка (check_send), затем либо выполнение fetch_and_send -> send_email, либо skip_email.
    check_send >> [fetch_and_send_task, skip_email]
    fetch_and_send_task >> send_email
