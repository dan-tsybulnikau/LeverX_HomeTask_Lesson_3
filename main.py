import argparse
import os
import sys
import json
from abc import ABC
from dotenv import load_dotenv
from mysql.connector import errorcode
import mysql.connector
from collections import OrderedDict
import xml.etree.ElementTree as tree

load_dotenv()

DB_CONFIG = {
    'host': os.environ.get('HOST'),
    'user': os.environ.get('USER'),
    'password': os.environ.get('PASSWORD'),
}


class CLIParser:

    def __init__(self):
        self.parser = argparse.ArgumentParser()

    def parse_args(self):
        self.parser.add_argument('source_to_rooms_file', type=str, action='store', nargs='?')
        self.parser.add_argument('source_to_students_file', type=str, action='store', nargs='?')
        self.parser.add_argument('output_format', type=str, action='store', nargs='?')
        args = self.parser.parse_args()
        if None in vars(args).values():
            raise SystemExit("Error: all arguments must be given")
        return args


class DBStartUp:

    def __connect_database(self):
        raise NotImplementedError

    def __create_database(self):
        raise NotImplementedError

    def create_tables(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError


class DataBaseStartUp(DBStartUp, ABC):
    TABLES = {
        'rooms': "CREATE TABLE rooms "
                 "(id INT NOT NULL PRIMARY KEY, "
                 "name VARCHAR(10) NOT NULL UNIQUE KEY"
                 ")",
        'students': "CREATE TABLE students "
                    "(id INT NOT NULL PRIMARY KEY, "
                    "name VARCHAR(100) NOT NULL, "
                    "birthday DATETIME NOT NULL, "
                    "sex ENUM('M','F') NOT NULL, "
                    "room INT NOT NULL, "
                    "CONSTRAINT room_fk FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE"
                    ")",
    }

    def __init__(self):
        self.db_name = os.environ.get('DATABASE')
        try:
            self.db = self.__create_database()
        except mysql.connector.errors.DatabaseError:
            print('Database {} already exists.'.format(self.db_name))
        self.db = self.__connect_database()
        self.cursor = self.db.cursor()

    def __create_database(self):
        rooms_and_students_database = mysql.connector.connect(**DB_CONFIG)
        cursor = rooms_and_students_database.cursor()
        cursor.execute("CREATE DATABASE rooms_and_students_database")
        sys.stdout.write('{} was created.\n'.format(self.db_name))
        cursor.close()
        return rooms_and_students_database

    def __connect_database(self):
        DB_CONFIG['database'] = self.db_name
        rooms_and_students_database = mysql.connector.connect(**DB_CONFIG)
        sys.stdout.write('{} was connected.\n'.format(self.db_name))
        return rooms_and_students_database

    def create_tables(self, *args):
        for table_name in args:
            table_description = DataBaseStartUp.__dict__.get('TABLES').get(table_name, None)
            try:
                self.cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("Table {} already exists.".format(table_name))
                else:
                    print(err.msg)
            else:
                print("Created table: {} ".format(table_name))

    def commit(self):
        self.db.commit()


class DataLoader:
    def add_data(self, *, data: list, db: DataBaseStartUp, table: str, columns: tuple):
        raise NotImplementedError


class DBDataLoader(DataLoader):
    def add_data(self, *, data: list, db: DataBaseStartUp, sql_table: str, sql_columns: tuple):
        cursor = db.cursor
        for row in data:
            num_of_params = f'({", ".join(("%s",) * len(sql_columns))})'
            value = tuple([row.get(i, None) for i in sql_columns])
            sql_msg = f"INSERT INTO {sql_table} ({', '.join(sql_columns)}) VALUES {num_of_params} "
            try:
                cursor.execute(sql_msg, value)
            except mysql.connector.errors.IntegrityError:
                continue
        db.commit()


class SqlQuery:
    SQL_MESSAGE = {
        'by_students': {
            'message': "SELECT rooms.*, COUNT(students.room) "
                       "FROM rooms LEFT JOIN students "
                       "ON rooms.id = students.room "
                       "GROUP BY rooms.id",
            'keys': ('room_id', 'room_name', 'number_of_students'),
        },
        'by_minimal_average_age': {
            'message': "SELECT rooms.*, CAST(AVG(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE())) as UNSIGNED) AS average "
                       "FROM rooms LEFT JOIN students "
                       "ON rooms.id = students.room "
                       "GROUP BY rooms.id "
                       "HAVING COUNT(students.room) > 0 "
                       "ORDER BY average "
                       "LIMIT 6",
            'keys': ('room_id', 'room_name', 'average_age'),
        },
        'by_age_difference': {
            'message': "SELECT rooms.*, MAX(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE()))-"
                       "MIN(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE())) AS diff "
                       "FROM rooms LEFT JOIN students "
                       "ON rooms.id = students.room "
                       "GROUP BY rooms.id "
                       "ORDER BY diff DESC "
                       "LIMIT 5",
            'keys': ('room_id', 'room_name', 'age_difference'),
        },
        'that_have_both_sex_students': {
            'message': "SELECT rooms.*, COUNT(students.room), "
                       "FORMAT(SUM(case when students.sex = 'F' then 1 else 0 end), 0) AS female, "
                       "FORMAT(SUM(case when students.sex = 'M' then 1 else 0 end), 0) AS male "
                       "FROM rooms LEFT JOIN students "
                       "ON rooms.id = students.room "
                       "GROUP BY rooms.id "
                       "HAVING male>0 and female>0 "
                       "ORDER BY rooms.id ",
            'keys': ('room_id', 'room_name', 'number_of_students', 'female_students_number', 'male_students_number'),
        },
    }

    def __init__(self, db: DataBaseStartUp):
        self.db = db

    def fetch(self, request: str):
        cursor = self.db.cursor
        cursor.execute(self.SQL_MESSAGE.get(request, None).get('message', None))
        db_response = cursor.fetchall()
        return db_response


class DataBaseExtractor:
    def __init__(self, db: DataBaseStartUp):
        self.db = db
        self.result = []
        self.query = SqlQuery(self.db)

    def extract(self, args: tuple):
        for arg in args:
            temp_result = self.query.fetch(arg)
            keys = self.query.SQL_MESSAGE.get(arg, None).get('keys', None)
            for i in range(len(temp_result)):
                temp_result[i] = OrderedDict(zip(keys, temp_result[i]))
            self.result.append({
                arg: temp_result
            })
        return self.result


class DataReader:
    def __init__(self):
        self.data = None

    def read_data(self, path: str):
        raise NotImplementedError


class JsonReader(DataReader):

    def __convert_data(self):
        return json.loads(self.data)

    def read_data(self, path: str):
        with open(path, mode='r') as data:
            self.data = data.read()
        return self.__convert_data()


class DataFormatter:
    def __init__(self, data):
        self.data = data

    def format_data(self):
        raise NotImplementedError


class DataToJsonFormatter(DataFormatter):
    def format_data(self):
        return json.dumps(self.data)


class DataToXmlFormatter(DataFormatter):
    def format_data(self):
        root = tree.Element('Result')
        for type_of_sort in self.data:
            h1 = tree.Element('sort')
            root.append(h1)
            for k in type_of_sort.keys():
                h2 = tree.SubElement(h1, 'sort_type_value')
                h2.text = f"{k}"
            for result_data in type_of_sort.values():
                for _ in result_data:
                    h3 = tree.SubElement(h2, 'data')
                    for k, v in list(_.items()):
                        h = tree.SubElement(h3, f'{k}')
                        h.text = f"{v}"
        final_tree = tree.ElementTree(root)
        return tree.tostring(final_tree.getroot()).decode("utf8")


class DataWriter:
    def __init__(self):
        self.formats = {'json': DataToJsonFormatter,
                        'xml': DataToXmlFormatter,
                        }

    def write_data(self, data: list, output_format: str):
        try:
            driver = self._select_driver(output_format)
        except ValueError:
            print("Not supported output format")
            exit()
        else:
            output_data = driver(data).format_data()
            with open(f'result.{output_format}', mode='w') as result_data:
                result_data.write(output_data)

    def _select_driver(self, output_format: str):
        if output_format not in self.formats:
            raise ValueError('Not')
        else:
            return self.formats.get(output_format)


def main():
    cli_parser = CLIParser()
    options = cli_parser.parse_args()

    my_db = DataBaseStartUp()
    my_db.create_tables('rooms', 'students')

    data_reader = JsonReader()
    rooms = data_reader.read_data(options.source_to_rooms_file)
    students = data_reader.read_data(options.source_to_students_file)

    data_loader = DBDataLoader()
    data_loader.add_data(data=rooms, db=my_db, sql_table='rooms', sql_columns=('id', 'name'))
    data_loader.add_data(data=students, db=my_db, sql_table='students',
                         sql_columns=('id', 'name', 'birthday', 'sex', 'room'))

    data_extractor = DataBaseExtractor(my_db)
    result = data_extractor.extract(tuple(data_extractor.query.SQL_MESSAGE.keys()))

    result_file_format = options.output_format.lower()
    output_data_writer = DataWriter()
    output_data_writer.write_data(data=result, output_format=result_file_format)


if __name__ == "__main__":
    main()
