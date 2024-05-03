import requests
import psycopg2


def get_employers(employers) -> list[dict]:
    """
        Получает данные по выбранным компаниям
        :return:
        """
    employers_data = []
    for i in employers:
        response = requests.get(f"https://api.hh.ru/employers/{i}")
        employers_data.append(response.json())
    return employers_data


def get_vacancies_employers(employers_data) -> list[dict]:
    """
    Получает список вакансий 10 выбранных компаний
    :return:
    """
    employers_vacancies = []
    for employer in employers_data:
        response = requests.get(employer['vacancies_url'], {'page': 0, 'per_page': 100})
        items_vacancies = response.json()['items']
        for i in items_vacancies:
            if i['salary'] is None:
                continue
            else:
                employers_vacancies.append(i)
    return employers_vacancies


def create_database(database_name, params_db):
    """Создание базы данных и таблиц"""

    conn = psycopg2.connect(**params_db)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'DROP DATABASE IF EXISTS {database_name}')
    cur.execute(f'CREATE DATABASE {database_name}')

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params_db)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers (
                    employers_id int PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    open_vacancies int,
                    vacancies_url VARCHAR(100)                   
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE vacancies (
                    id_vacancies int PRIMARY KEY,
                    employer VARCHAR(50) REFERENCES employers(name),
                    name_vacancies VARCHAR(200) NOT NULL,
                    salary_from int,
                    salary_to int,
                    salary_currency VARCHAR(10),
                    url_vacancy VARCHAR(100)
                )
            """)

    conn.commit()
    conn.close()


def insert_tables(database_name, params_db, employers_vacancies, employers_data):
    with psycopg2.connect(dbname=database_name, **params_db) as conn:
        with conn.cursor() as cur:
            for employer in employers_data:
                cur.execute('INSERT INTO employers (employers_id, name, open_vacancies, vacancies_url) '
                            'VALUES (%s, %s, %s, %s) returning *',
                            (employer['id'], employer['name'], employer['open_vacancies'], employer['vacancies_url']))
            for vacancy in employers_vacancies:
                cur.execute('INSERT INTO vacancies (id_vacancies, employer, name_vacancies, salary_from, salary_to, '
                            'salary_currency, url_vacancy) VALUES (%s, %s, %s, %s, %s, %s, %s) returning *',
                            (vacancy['id'], vacancy['employer']['name'], vacancy['name'], vacancy['salary']['from'],
                             vacancy['salary']['to'], vacancy['salary']['currency'], vacancy['url']))

    conn.close()
