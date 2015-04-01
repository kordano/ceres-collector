#!/bin/bash
t=$2
d=$1
mongoimport --db juno --collection tweets $t/tweets-$d.json
mongoimport --db juno --collection tags $t/tags-$d.json
mongoimport --db juno --collection users $t/users-$d.json
mongoimport --db juno --collection urls $t/urls-$d.json
mongoimport --db juno --collection htmls $t/htmls-$d.json
mongoimport --db juno --collection messages $t/messages-$d.json
mongoimport --db juno --collection mentions $t/mentions-$d.json
mongoimport --db juno --collection sources $t/sources-$d.json
mongoimport --db juno --collection replies $t/replies-$d.json
mongoimport --db juno --collection retweets $t/retweets-$d.json
mongoimport --db juno --collection shares $t/shares-$d.json
mongoimport --db juno --collection pubs $t/pubs-$d.json
mongoimport --db juno --collection urlrefs $t/urlrefs-$d.json
mongoimport --db juno --collection tagrefs $t/tagrefs-$d.json
mongoimport --db juno --collection unknown $t/unknown-$d.json
