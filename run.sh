#!/bin/bash
docker run \
--name mictlanx-router-0 \
--network="mictlanx" \
-e MICTLANX_PEERS="peer-0:148.247.201.141:7000 peer-1:148.247.201.226:7001" \
-p 60666:60666 \
-d \
nachocode/mictlanx:router
