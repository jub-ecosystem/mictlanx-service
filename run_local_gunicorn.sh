#!/bin/bash

export MICTLANX_ROUTER_HOST=0.0.0.0
export MICTLANX_ROUTER_PORT=60666

WORKERS=${1:-1}

gunicorn mictlanxrouter.server:app \
	  --worker-class uvicorn.workers.UvicornWorker \
	    --bind $MICTLANX_ROUTER_HOST:$MICTLANX_ROUTER_PORT \
	      --workers ${WORKERS}
	    #--bind ${MICTLANX_ROUTER_HOST}:${MICTLANX_ROUTER_PORT} \
