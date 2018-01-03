#!/usr/bin/env bash
HOST="localhost"
USER="postgres"
PASSWORD="postgres"
ROOT=$(dirname "$BASH_SOURCE")
cd "$ROOT"/..

PGPASSWORD="$PASSWORD" psql -U "$USER" -h "$HOST" -d "$USER" < dbo/schema.sql
xo -X --single-file -o dbo/ "pgsql://$USER:$PASSWORD@$HOST/$USER?sslmode=disable"
sed -i 's/"public".//g' dbo/dbo.xo.go
go-bindata -pkg dbo -prefix dbo/ -o dbo/schema.bin.go dbo/schema.sql