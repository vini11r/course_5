import psycopg2


class DBManager:

    def __init__(self, database_name, params_db):
        self.conn = psycopg2.connect(dbname=database_name, **params_db)

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """
        cur = self.conn.cursor()
        cur.execute('SELECT name, open_vacancies FROM employers')
        return cur.fetchall()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию
        """
        cur = self.conn.cursor()
        cur.execute('SELECT employer, name_vacancies, salary_from, salary_to, salary_currency, url_vacancy '
                    'FROM vacancies')
        return cur.fetchall()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям
        """
        cur = self.conn.cursor()
        cur.execute(' SELECT ROUND(AVG(salary_from + salary_to) / 2::numeric, 2) '
                    'FROM vacancies')
        return cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям
        """
        cur = self.conn.cursor()
        cur.execute(' SELECT * FROM vacancies '
                    'WHERE salary_to > (SELECT ROUND(AVG (salary_from + salary_to) / 2::numeric, 2) FROM vacancies)')
        return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python
        """
        cur = self.conn.cursor()
        cur.execute(f"""SELECT * FROM vacancies WHERE name_vacancies ILIKE '%{keyword}%'""")
        return cur.fetchall()

    def close(self):
        self.conn.close()


