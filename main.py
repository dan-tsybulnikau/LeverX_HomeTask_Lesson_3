from abc import ABC, abstractmethod
import argparse
from collections import OrderedDict
import logging
import os
import json
import sys
import xml.etree.ElementTree as Tree
from dotenv import load_dotenv
from mysql.connector import errorcode
import mysql.connector

load_dotenv()

logging.basicConfig(
    filename='database.log',
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s - %(module)s - %(levelname)s - %(message)s')


class MyException(Exception):
    pass


class FileDoesNotExists(MyException):
    def __init__(self, message):
        super().__init__()
        self.message = message


class NotSupportedOutputFormat(MyException):
    def __init__(self, message):
        super().__init__()
        self.message = message


class OutputFormatCheck(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values not in DataWriter.OUTPUT_FORMATS.keys():
            raise NotSupportedOutputFormat("%s is not supported as output file format" % values)
        setattr(namespace, self.dest, values)


class FileExistsCheck(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if not os.path.exists(values):
            raise FileDoesNotExists('File %s is not found' % values)
        setattr(namespace, self.dest, values)


class CLIParser:

    def __init__(self):
        self.parser = argparse.ArgumentParser()

    def parse_args(self):
        self.parser.add_argument('source_to_rooms_file',
                                 help="Source to file with  rooms",
                                 type=str, action=FileExistsCheck,
                                 nargs='?', )
        self.parser.add_argument('source_to_students_file',
                                 help="Source to file with students",
                                 type=str, action=FileExistsCheck,
                                 nargs='?')
        self.parser.add_argument('output_format',
                                 help="Output file format",
                                 type=str,
                                 action=OutputFormatCheck,
                                 nargs='?')
        self.parser.add_argument("--db-name",
                                 help="Database name",
                                 type=str,
                                 nargs=1,
                                 action='store')
        self.parser.add_argument("--db-host",
                                 help="Database host",
                                 type=str,
                                 nargs=1,
                                 action='store')
        self.parser.add_argument("--db-user",
                                 help="Database user",
                                 type=str,
                                 nargs=1,
                                 action='store')
        self.parser.add_argument("--db-password",
                                 help="Database password",
                                 type=str,
                                 nargs=1,
                                 action='store')
        try:
            self.args = self.parser.parse_args()
        except (FileDoesNotExists, NotSupportedOutputFormat) as error:
            logging.error(error.message)
            sys.exit()
        return self.args

    def retrieve_db_info(self):
        database_params = {
            'db_name': self.args.db_name[0] if self.args.db_name else os.environ.get('DATABASE'),
            'host': self.args.db_host[0] if self.args.db_host else os.environ.get('HOST'),
            'user': self.args.db_password[0] if self.args.db_password else os.environ.get('PASSWORD'),
            'password': self.args.db_password[0] if self.args.db_password else os.environ.get('PASSWORD')
        }
        print(database_params)
        return database_params


class DBEngine(ABC):

    @abstractmethod
    def connect_database(self):
        pass

    @abstractmethod
    def create_database(self):
        pass

    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def commit(self):
        pass


class DataBaseEngine(DBEngine):
    TABLES = {
        'rooms': """CREATE TABLE rooms 
                        (
                            id INT NOT NULL PRIMARY KEY, 
                            name VARCHAR(10) NOT NULL UNIQUE KEY
                        )""",
        'students': """CREATE TABLE students 
                        (
                            id INT NOT NULL PRIMARY KEY, 
                            name VARCHAR(100) NOT NULL, 
                            birthday DATETIME NOT NULL, 
                            sex ENUM('M','F') NOT NULL, 
                            room INT NOT NULL, 
                            CONSTRAINT room_fk FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE
                        )""",
    }

    def __init__(self, **kwargs):
        self.db_name = kwargs['db_name']
        self.host = kwargs['host']
        self.user = kwargs['user']
        self.password = kwargs['password']
        try:
            self.db = self.create_database()
        except mysql.connector.errors.DatabaseError:
            logging.info('Database {} already exists.'.format(self.db_name.title()))
        self.db = self.connect_database()
        self.cursor = self.db.cursor()

    def create_database(self):
        database = mysql.connector.connect(user=self.user,
                                           host=self.host,
                                           password=self.password)
        cursor = database.cursor()
        cursor.execute("CREATE DATABASE %s" % self.db_name)
        logging.info('{} was created.'.format(self.db_name.title()))
        cursor.close()
        return database

    def connect_database(self):
        database = mysql.connector.connect(user=self.user,
                                           host=self.host,
                                           password=self.password,
                                           database=self.db_name)
        logging.info('{} was connected.'.format(self.db_name.title()))
        return database

    def create_tables(self, *args):
        for table_name in args:
            table_description = DataBaseEngine.__dict__.get('TABLES').get(table_name, None)
            try:
                self.cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logging.info("Table {} already exists.".format(table_name))
                else:
                    logging.error(err.msg)
            else:
                logging.info("Created table: {} ".format(table_name))

    def commit(self):
        self.db.commit()


class DataLoader(ABC):
    @abstractmethod
    def add_data(self, *, data: list, db: DataBaseEngine, table: str, columns: tuple):
        pass


class DBDataLoader(DataLoader):
    def add_data(self, *, data: list, db: DataBaseEngine, sql_table: str, sql_columns: tuple):
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
            'message': """
                        SELECT  rooms.*, 
                                COUNT(students.room)
                        FROM    rooms 
                                LEFT JOIN students
                                       ON rooms.id = students.room
                        GROUP   BY rooms.id
                        """,
            'keys': ('room_id', 'room_name', 'number_of_students'),
        },
        'by_minimal_average_age': {
            'message': """
                        SELECT  rooms.*, 
                                CAST(AVG(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE())) as UNSIGNED) 
                                AS 
                                average
                        FROM    rooms 
                                LEFT JOIN students
                                    ON rooms.id = students.room
                        GROUP   BY rooms.id
                        HAVING  COUNT(students.room) > 0
                        ORDER   BY average
                        LIMIT   6
                        """,
            'keys': ('room_id', 'room_name', 'average_age'),
        },
        'by_age_difference': {
            'message': """
                        SELECT  rooms.*, 
                                MAX(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE()))-
                                MIN(TIMESTAMPDIFF(YEAR, students.birthday, CURDATE())) AS diff
                        FROM    rooms 
                                LEFT JOIN students
                                       ON rooms.id = students.room
                        GROUP   BY rooms.id
                        ORDER   BY diff DESC
                        LIMIT   5
                        """,
            'keys': ('room_id', 'room_name', 'age_difference'),
        },
        'that_have_both_sex_students': {
            'message': """
                        SELECT  rooms.*, 
                                COUNT(students.room),
                                FORMAT(SUM(CASE 
                                            when students.sex = 'F' then 1 else 0 end), 0) AS female,
                                FORMAT(SUM(case 
                                            when students.sex = 'M' then 1 else 0 end), 0) AS male
                        FROM    rooms 
                                LEFT JOIN students 
                                       ON rooms.id = students.room
                        GROUP   BY rooms.id
                        HAVING  male>0 
                                and female>0
                        ORDER   BY rooms.id
                        """,
            'keys': ('room_id', 'room_name', 'number_of_students', 'female_students_number', 'male_students_number'),
        },
    }

    def __init__(self, db: DataBaseEngine):
        self.db = db

    def fetch(self, request: str):
        cursor = self.db.cursor
        cursor.execute(self.SQL_MESSAGE.get(request, None).get('message', None))
        db_response = cursor.fetchall()
        return db_response


class DataBaseExtractor:
    def __init__(self, db: DataBaseEngine):
        self.db = db
        self.result = []
        self.query = SqlQuery(self.db)

    def extract(self, args: tuple):
        for arg in args:
            temp_result = self.query.fetch(arg)
            keys = self.query.SQL_MESSAGE.get(arg, None).get('keys', None)
            for i, value in enumerate(temp_result):
                temp_result[i] = OrderedDict(zip(keys, value))
            self.result.append({
                arg: temp_result
            })
        logging.info('Queries were completed.')
        return self.result


class FileReader:
    @staticmethod
    def read_file(path: str, file_mode='r'):
        with open(path, mode=file_mode) as file:
            file_data = file.read()
        return file_data


class DataReader(ABC):
    @abstractmethod
    def read_data(self, path: str):
        pass


class JsonReader(DataReader):
    def read_data(self, data: str):
        return json.loads(data)


class DataFormatter(ABC):
    def __init__(self, data):
        self.data = data

    @abstractmethod
    def format_data(self):
        pass


class DataToJsonFormatter(DataFormatter):
    def format_data(self):
        return json.dumps(self.data)


class DataToXmlFormatter(DataFormatter):
    def format_data(self):
        root = Tree.Element('Result')
        for type_of_sort in self.data:
            h1 = Tree.Element('sort')
            root.append(h1)
            for k in type_of_sort.keys():
                h2 = Tree.SubElement(h1, 'sort_type_value')
                h2.text = f"{k}"
            for result_data in type_of_sort.values():
                for _ in result_data:
                    h3 = Tree.SubElement(h2, 'data')
                    for k, v in list(_.items()):
                        h = Tree.SubElement(h3, f'{k}')
                        h.text = f"{v}"
        final_tree = Tree.ElementTree(root)
        return Tree.tostring(final_tree.getroot()).decode("utf8")


class DataWriter:
    OUTPUT_FORMATS = {
        'json': DataToJsonFormatter,
        'xml': DataToXmlFormatter,
    }

    def write_data(self, data: list, output_format: str):
        driver = self.__select_driver(output_format)
        output_data = driver(data).format_data()
        with open(f'result.{output_format}', mode='w') as result_data:
            result_data.write(output_data)
        logging.info(f'File "result.{output_format}" was created.')

    @staticmethod
    def __select_driver(output_format: str):
        return DataWriter.OUTPUT_FORMATS.get(output_format)


def main():
    cli_parser = CLIParser()
    options = cli_parser.parse_args()

    database_params = cli_parser.retrieve_db_info()

    my_db = DataBaseEngine(**database_params)
    my_db.create_tables('rooms', 'students')

    rooms_file_data = FileReader.read_file(options.source_to_rooms_file)
    students_file_data = FileReader.read_file(options.source_to_students_file)

    data_reader = JsonReader()
    rooms = data_reader.read_data(rooms_file_data)
    students = data_reader.read_data(students_file_data)

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
