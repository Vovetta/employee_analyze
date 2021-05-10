from sqlite3 import connect

from utils import process_employees_data, load_data, process_projects_data, process_days_data, export_csv


def extract_data(dir_name: str, db_name: str) -> None:
    """
    Main function for data extracting and exporting to csv

    :param dir_name: path to data directory
    :param db_name: path to SQLite database, if doesn't exist - creates it
    """
    connection = connect(db_name)

    concatenated = load_data(dir_name)

    employees = process_employees_data(connection, concatenated)
    projects = process_projects_data(connection, concatenated, employees)
    process_days_data(connection, concatenated, employees, projects)

    export_csv(connection)

    connection.close()


if __name__ == '__main__':
    extract_data('data', 'temp.db')
