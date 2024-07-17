#!/bin/bash
readonly IMAGE_TAG=${1:-router}
docker build -f ./Dockerfile -t nachocode/mictlanx:${IMAGE_TAG} .
