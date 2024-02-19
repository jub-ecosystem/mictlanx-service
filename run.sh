#!/bin/bash
docker run \
--name mictlanx-router-0 \
--network="mictlanx" \
-p 60666:60666 \
-d \
nachocode/mictlanx:router
