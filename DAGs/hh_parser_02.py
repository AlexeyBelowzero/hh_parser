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


def should_send():
    today = date.today()
    current_year = today.year
    current_week = today.isocalendar()[1]  # номер недели по ISO
    var_key = "vacancies_sent_week"
    default_data = {"year": current_year, "week": current_week, "count": 0}
    # Получаем переменную с дефолтным значением, если её ещё нет
    data = Variable.get(var_key, default_var=json.dumps(default_data), deserialize_json=True)

    # Если данные относятся к другой неделе/году, сбрасываем счётчик
    if data.get("year") != current_year or data.get("week") != current_week:
        data = default_data
        Variable.set(var_key, json.dumps(data))

    if data.get("count", 0) < 3:
        return "fetch_and_send"
    else:
        return "skip_email"


def fetch_and_send(ti, **kwargs):
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "Data engineer",  # поисковый запрос
        "area": 1,               # код региона (можно изменить)
        "per_page": 20,          # число вакансий на странице
    }

    # Выполняем GET-запрос к API
    response = requests.get(url, params=params)
    response.raise_for_status()  # при ошибке запрос вызовет исключение
    data = response.json()
    vacancies = data.get("items", [])

    if not vacancies:
        vacancy_html = "<p>Новых вакансий не найдено.</p>"
    else:
        # Для простоты выбираем первую вакансию
        vacancy = vacancies[0]
        name = vacancy.get("name")
        employer = vacancy.get("employer", {}).get("name", "Неизвестно")
        link = vacancy.get("alternate_url")
        vacancy_html = (
            f"<h3>Новая вакансия Data engineer:</h3>"
            f"<p><b>{name}</b><br>"
            f"Компания: {employer}<br>"
            f"<a href='{link}'>Подробнее</a></p>"
        )

    # Обновляем переменную, увеличивая счётчик отправленных вакансий
    var_key = "vacancies_sent_week"
    today = date.today()
    current_year = today.year
    current_week = today.isocalendar()[1]
    default_data = {"year": current_year, "week": current_week, "count": 0}
    data_var = Variable.get(var_key, default_var=json.dumps(default_data), deserialize_json=True)
    count = data_var.get("count", 0)
    data_var["count"] = count + 1
    Variable.set(var_key, json.dumps(data_var))

    # Передаём сформированный HTML через XCom для EmailOperator
    ti.xcom_push(key="vacancy_html", value=vacancy_html)


# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hh_vacancies_test_dag_2',
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

    # Задача для получения вакансии и подготовки письма
    fetch_and_send_task = PythonOperator(
        task_id='fetch_and_send',
        python_callable=fetch_and_send,
    )

    # Задача отправки email с полученной вакансией
    send_email = EmailOperator(
        task_id='send_email',
        to='alexeybelozerov93@gmail.com',
        subject='Новая вакансия Data engineer',
        html_content="{{ ti.xcom_pull(task_ids='fetch_and_send', key='vacancy_html') }}",
    )

    # Задача-заглушка, если лимит вакансий уже достигнут
    skip_email = DummyOperator(
        task_id='skip_email'
    )

    # Определяем последовательность выполнения:
    # Сначала проверка, затем либо получение вакансии и отправка email, либо пропуск.
    check_send >> [fetch_and_send_task, skip_email]
    fetch_and_send_task >> send_email
