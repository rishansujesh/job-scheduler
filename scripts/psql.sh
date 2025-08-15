#!/bin/sh
docker exec -it js-postgres psql -U "${POSTGRES_USER:-jobs}" -d "${POSTGRES_DB:-jobs}"
