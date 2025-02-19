#!/bin/bash

hypercorn mictlanxrouter.server:app --bind 0.0.0.0:60666

