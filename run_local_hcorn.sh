#!/bin/bash
poetry run hypercorn mictlanxrouter.server:app --bind 0.0.0.0:60666 --certfile=pems/cert.pem --keyfile=pems/key.pem --workers 1
