`pip install -r requirements.txt`
`docker run -d --name ht_pg_server -e POSTGRES_HOST_AUTH_METHOD=trust -v ht_dbdata:/var/lib/postgresql/data -p 54320:5432 postgres:11`
`docker logs -f ht_pg_server`
`./form_data_set.sh`
