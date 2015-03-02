#!/bin/bash
docker stop ceres-collector; docker rm ceres-collector;  docker run -d -v /home/konrad/data/ceres-collector:/opt/data --link mongodb:db --name ceres-collector kordano/ceres-collector
