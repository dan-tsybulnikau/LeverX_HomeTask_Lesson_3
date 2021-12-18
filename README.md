# LeverX_HomeTask_Lesson_3

Rooms and Students DataBase app based on argpare and MySQL

Main features: 
1) Creates new DB if not exits, else connects to existion
2) Allows to create tables with specified names
3) Writes data from rooms file and students file
4) Extracts data according to specified rules (inside Query.SQL_MESSAGE keys)
5) Saves data as result.json or result.xml files

App supports logging info into 'database.log' file.

Two basic ways of using this app:
1) With positional arguments only. In this case, configuration af database is based on .env file
2) With positional and optional arguments. In this way, user provider database name, host, user name and password via  arguments
--db-name, --db-user, --db-host, --db-password
