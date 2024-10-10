include .env

to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" "${MYSQL_DATABASE}"

to_mysql_root:
	docker exec -it de_mysql mysql -u"${MYSQL_ROOT_USER}" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_create:
	docker exec -it de_mysql mysql --local_infile=1 -h127.0.0.1 -u"${MYSQL_ROOT_USER}" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/create_database.sql"

mysql_load:
	docker exec -it de_mysql mysql --local_infile=1 -h127.0.0.1 -u"${MYSQL_ROOT_USER}" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/load_data.sql"