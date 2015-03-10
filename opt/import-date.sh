#!/bin/bash
d=$1
mongoimport --db juno --collection tweets tweets-$d.json
mongoimport --db juno --collection refs refs-$d.json
mongoimport --db juno --collection tags tags-$d.json
mongoimport --db juno --collection users users-$d.json
mongoimport --db juno --collection urls urls-$d.json
mongoimport --db juno --collection htmls htmls-$d.json
mongoimport --db juno --collection messages messages-$d.json
mongoimport --db juno --collection mentions refs-$d.json
mongoimport --db juno --collection sources refs-$d.json
mongoimport --db juno --collection replies refs-$d.json
mongoimport --db juno --collection retweets refs-$d.json
mongoimport --db juno --collection shares refs-$d.json
mongoimport --db juno --collection pubs refs-$d.json
mongoimport --db juno --collection urlrefs refs-$d.json
mongoimport --db juno --collection tagrefs refs-$d.json
mongoimport --db juno --collection unknown refs-$d.json
