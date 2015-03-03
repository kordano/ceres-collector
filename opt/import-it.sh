#!/bin/bash
# Import yesterday's collected data, data path as argument
yesterday=$(date -d yesterday +'%Y-%m-%d')
data=$1
mongoimport --db juno --collection tweets $data/tweets-$yesterday.json
mongoimport --db juno --collection refs $data/refs-$yesterday.json
mongoimport --db juno --collection tags $data/tags-$yesterday.json
mongoimport --db juno --collection users $data/users-$yesterday.json
mongoimport --db juno --collection urls $data/urls-$yesterday.json
mongoimport --db juno --collection htmls $data/htmls-$yesterday.json
mongoimport --db juno --collection messages $data/messages-$yesterday.json
