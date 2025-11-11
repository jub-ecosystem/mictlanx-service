#!/bin/bash
./clean_dist.sh
poetry remove mictlanxrm
poetry add ~/Programming/Python/mictlanx-healer/dist/mictlanxrm-$1.tar.gz
