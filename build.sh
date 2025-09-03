#!/bin/bash
readonly IMAGE_TAG=${1:-0.0.161a20}
readonly IMAGE=nachocode/mictlanx:router-${IMAGE_TAG}
readonly PUSH_FLAG=${2:-1}



docker build -f ./Dockerfile -t $IMAGE .

if [ "$PUSH_FLAG" -eq 1 ]; then
	echo "Pushing image: $IMAGE"
	docker push $IMAGE
else
	echo "Skipping push"
fi 
