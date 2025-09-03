#!/bin/bash
poetry remove mictlanx
readonly VERSION=${1:-0.0.157}
cp ~/Programming/Python/mictlanx/dist/mictlanx-${VERSION}.tar.gz mictlanx.tar.gz
poetry add ./mictlanx.tar.gz
