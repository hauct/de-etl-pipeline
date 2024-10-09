include .env

to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p "${MYSQL_PASSWORD}" "${MYSQL_DATABASE}"