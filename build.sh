#!/bin/bash
readonly IMAGE_TAG=${1:-0.0.161a20}
readonly IMAGE=nachocode/mictlanx:router-${IMAGE_TAG}
docker build -f ./Dockerfile -t $IMAGE .
docker push $IMAGE
