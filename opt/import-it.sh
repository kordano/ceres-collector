#!/bin/bash
# Import yesterday's collected data, data path as argument
yesterday=$(date -d yesterday +'%Y-%m-%d')
data=$1
mongoimport --db demeter --collection tweets $data/tweets-$yesterday.json
mongoimport --db demeter --collection refs $data/refs-$yesterday.json
mongoimport --db demeter --collection tags $data/tags-$yesterday.json
mongoimport --db demeter --collection users $data/users-$yesterday.json
mongoimport --db demeter --collection urls $data/urls-$yesterday.json
mongoimport --db demeter --collection htmls $data/htmls-$yesterday.json
mongoimport --db demeter --collection messages $data/messages-$yesterday.json
