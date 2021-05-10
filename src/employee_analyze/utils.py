import glob
from sqlite3 import Connection
from typing import List

from pandas import (
    read_csv, read_sql_query,
    concat, merge,
    DataFrame
)


def get_all_files(dir_name: str) -> List[str]:
    """
    Get all data files in directory

    :param dir_name: path to data directory
    :return: list of file paths
    """
    files = glob.glob(f'{dir_name}/*.csv')
    return files


def load_data(dir_name: str) -> DataFrame:
    """
    Loads and preprocesses all data from directory

    :param dir_name: path to data directory
    :return: data concatenated in DataFrame
    """
    all_data_frames = []
    for csv_file in get_all_files(dir_name):
        data = read_csv(csv_file, sep=';', parse_dates=[2]).melt(
            id_vars=['Название проекта', 'Руководитель', 'Дата сдачи'],
            var_name='employee',
            value_name='days'
        )
        all_data_frames.append(data)

    concatenated = concat(all_data_frames)
    concatenated.rename(columns={
        'Название проекта': 'project_name',
        'Руководитель': 'head',
        'Дата сдачи': 'completion_date'
    }, inplace=True)
    return concatenated


def process_employees_data(connection: Connection, data: DataFrame) -> DataFrame:
    """
    Finds all unique employees and inserts to SQLite

    :param connection: connection to SQLite
    :param data: all data in DataFrame
    :return: unique employees
    """
    employees = DataFrame({'employee_name': data['employee'].unique()})
    employees.to_sql('employee', connection, if_exists='replace')

    return employees


def process_projects_data(connection: Connection, data: DataFrame, employees: DataFrame) -> DataFrame:
    """
    Find all unique projects and preprocesses heads, inserts to SQLite

    :param connection: connection to SQLite
    :param data: all data in DataFrame
    :param employees: unique employees
    :return: unique projects
    """
    employees['head_id'] = employees.index

    concatenated = merge(data, employees, how='left', left_on='head', right_on='employee_name')
    projects = concatenated[
        ['project_name', 'head_id', 'completion_date']
    ].drop_duplicates().reset_index(drop=True)
    projects.to_sql('projects', connection, if_exists='replace')

    return projects


def process_days_data(connection: Connection, data: DataFrame, employees: DataFrame, projects: DataFrame) -> None:
    """
    Preprocesses data with ids of employees and projects, inserts to SQLite

    :param connection: connection to SQLite
    :param data: all data in DataFrame
    :param employees: unique employees
    :param projects: unique projects
    """
    projects['project_id'] = projects.index
    employees['employee_id'] = employees.index

    concatenated = merge(data, projects, how='left', on='project_name')
    concatenated = merge(concatenated, employees, how='left', left_on='employee', right_on='employee_name')

    worker_days = concatenated[['project_id', 'employee_id', 'days']].dropna()
    worker_days.to_sql('worker_days', connection, if_exists='replace')


def export_csv(connection: Connection) -> None:
    """
    Exporting data from SQLite to csv

    :param connection: connection to SQLite
    """
    # Projects by completion date
    projects_df = read_sql_query("""
        select
            project_name as 'Проект',
            completion_date as 'Дата сдачи'
        from projects
        order by completion_date desc
    """, connection)
    projects_df.to_csv('all_projects.csv', sep=';', index_label='№')

    # Employee working days on a project
    worker_days_df = read_sql_query("""
        select
            project_name as 'Проект',
            employee_name as 'Сотрудник',
            days as 'Количество дней'
        from worker_days
        left join employee on employee."index" = worker_days.employee_id
        left join projects on projects."index" = worker_days.project_id
        where days > 0
        order by completion_date desc
    """, connection)
    worker_days_df.to_csv('worker_days.csv', sep=';', index_label='№')

    # Employee working days on all projects
    days_by_worker_df = read_sql_query("""
        select
            employee_name as 'Сотрудник',
            sum(days) as 'Количество дней'
        from worker_days
        left join employee on employee."index" = worker_days.employee_id
        group by employee_name
        order by sum(days) desc
    """, connection)
    days_by_worker_df.to_csv('days_by_worker.csv', sep=';', index_label='№')
