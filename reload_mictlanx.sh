#!/bin/bash
poetry remove mictlanx
readonly VERSION=${1:-0.0.157}
poetry add ~/Programming/Python/mictlanx/dist/mictlanx-${VERSION}.tar.gz
