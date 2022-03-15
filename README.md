## Компонент для группировки поисковых запросов по городам (проектам) и типу запроса (searchType)

## Запросы к диггеру
* Для каждого месяца необходимо выполниить запрос к сервису статистики аналогичный данному: https://digger.web-staging.2gis.ru/detailed?id=396296
* Распаковать архивы в директорию `/.../queries_data_set_former/data/`, можно использовать команду `find . -name '*.csv.gz' -print0 | xargs -0 -n1 gzip -d`
* Переименовать файлы, чтобы можно было легко понимать, за какой месяц данные.

## Создание базы данных
Мы будем использовать PostgreSQL 11 сервер и Docker. Установим Docker: https://docs.docker.com/. 

Выполним следующие команды:
0. Установим зависимости `pip install -r requirements.txt`
1. Запуск сервера:
`docker run -d --name ht_pg_server -e POSTGRES_HOST_AUTH_METHOD=trust -v ht_dbdata:/var/lib/postgresql/data -p 54320:5432 postgres:11`
2. Проверим логи, что он запущен:
`docker logs -f ht_pg_server`
3. Загрузим данные в базу: `./form_data_set.sh`

------------------------------------------------------------------------------------------------------------------------

## Дополнительные команды:
`sudo docker ps -a` - список запущенных докер-образов.
`sudo docker rm -f CONTAINER ID` - остановить запущенный докер-образ
`sudo lsof -i` - список портов
`sudo kill -9 PID` - остановка процесса
`docker exec -it ht_pg_server psql -U postgres -c "dROP database main_db"` - удалим базу данных: 
Размер базы данных: 
* `docker exec -it ht_pg_server psql -U postgres`
* `\l+`
* альтернатива: 
    * `\c test_db` подключение к бд;
    * `\dt+`