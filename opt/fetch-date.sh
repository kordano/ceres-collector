#!/bin/bash
t=$2
d=$1
scp -P 2023 konrad@mono.topiq.es:/home/konrad/data/ceres-collector/*-$d.json $t
