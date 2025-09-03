#!/bin/bash
./build.sh $1
docker compose -p mictlanx -f ./router.yml down
docker compose -p mictlanx -f ./router.yml up -d
