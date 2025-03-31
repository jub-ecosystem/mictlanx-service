#!/bin/bash
readonly IMAGE_TAG=${1:-router}
readonly IMAGE=nachocode/mictlanx:router-${IMAGE_TAG}
docker build -f ./Dockerfile -t $IMAGE .
docker push $IMAGE
