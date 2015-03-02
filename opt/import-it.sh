#!/bin/bash
# Import yesterday's collected data, data path as argument
yesterday=$(date -d yesterday +'%Y-%m-%d')
data=$1
mongoimport --db hera --collection tweets $data/tweets-$yesterday.json
mongoimport --db hera --collection refs $data/refs-$yesterday.json
mongoimport --db hera --collection tags $data/tags-$yesterday.json
mongoimport --db hera --collection users $data/users-$yesterday.json
mongoimport --db hera --collection urls $data/urls-$yesterday.json
mongoimport --db hera --collection htmls $data/htmls-$yesterday.json
mongoimport --db hera --collection messages $data/messages-$yesterday.json
