#!/bin/bash

docker pull postgres

sleep 10

docker run --rm --name test_db -e POSTGRES_PASSWORD=@sde_password012 -e POSTGRES_USER=test_sde -e POSTGRES_DB=demo -d -p 5432:5432 -v $(pwd)/sql:/var/lib/postgresql/test_sql postgres

sleep 10

docker exec test_db psql demo -U test_sde -f /var/lib/postgresql/test_sql/init_db/demo.sql

# тест calc.sql
# docker exec test_db psql demo -U test_sde -f /var/lib/postgresql/test_sql/main/calc.sql

