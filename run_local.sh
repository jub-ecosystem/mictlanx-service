#!/bin/bash

uvicorn mictlanxrouter.server:app --host ${MICTLANX_ROUTER_HOST-0.0.0.0} --port ${MICTLANX_ROUTER_PORT-60666} --reload --workers $1

