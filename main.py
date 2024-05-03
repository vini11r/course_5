from DBManager import DBManager
from utils import get_employers, get_vacancies_employers, insert_tables, create_database
from config import config


def main():
    employers = [1740, 5008932, 4986323, 566, 2748, 533809, 2562304, 1838, 64474, 15478]
    database_name = 'database_vacancies'
    params_db = config()

    employers_data = get_employers(employers)
    employers_vacancies = get_vacancies_employers(employers_data)

    create_database(database_name, params_db)
    insert_tables(database_name, params_db, employers_vacancies, employers_data)

    db = DBManager(database_name, params_db)

    print("""
        Для получения списка всех компаний и количества их вакансий введите - 1;
        Для получения вакансий (только вакансии с указанной зарплатой) введите - 2;
        Для получения средней зарплаты по всем вакансиям ведите - 3;
        Для получения вакансий у которых зарплата выше общей средней введите - 4;
        Для поиска по вакансиям введите - 5""")

    user_answer = input('Введите Ваш запрос:  ')

    if user_answer == '1':
        companies_and_vacancies = db.get_companies_and_vacancies_count()
        for i in companies_and_vacancies:
            print(*i)

    elif user_answer == '2':
        all_vacancies = db.get_all_vacancies()
        for v in all_vacancies:
            print(*v)

    elif user_answer == '3':
        avg_salary = db.get_avg_salary()
        for f in avg_salary:
            print(*f)

    elif user_answer == '4':
        vacancies_salary_higher_avg = db.get_vacancies_with_higher_salary()
        for a in vacancies_salary_higher_avg:
            print(*a)

    elif user_answer == '5':
        user_input = input('Введите поисковый запрос:  ')
        vacancies_keyword = db.get_vacancies_with_keyword(user_input)
        for a in vacancies_keyword:
            print(*a)
    else:
        db.close()
        main()

    db.close()


if __name__ == '__main__':
    main()
