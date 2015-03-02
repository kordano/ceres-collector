#!/bin/bash
yesterday=$(date -d yesterday +'%Y-%m-%d')
target=$1
scp -P 2023 konrad@mono.topiq.es:/home/konrad/data/ceres-collector/*-$yesterday.json $target
