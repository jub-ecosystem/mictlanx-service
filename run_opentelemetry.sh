#!/bin/bash
docker compose -f ./opentelemetry/docker-compose.yml down
docker compose -f ./opentelemetry/docker-compose.yml up -d

