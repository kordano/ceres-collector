# ceres-collector

An article and related tweets collector.

## Usage

A running Mongodb instance is required.

Configure server (see '/opt/server-config.edn' for reference) and start it with

```
lein run opt/server-config.edn
```
## Docker

Build it
```
sudo docker build --rm -t kordano/ceres-collector .
```

Install and run [dockerfile/mongodb](https://index.docker.io/u/dockerfile/mongodb/ "dockerfile/mongodb") if not installed
```
sudo docker pull dockerfile/mongodb 
sudo docker run -d -p 27017:27017 --name mongodb dockerfile/mongodb
```

Fill in twitter credentials and other server configuration (e.g. port, build, ...) in `opt/server-config.edn` on the local machine where docker is running. It is recommended not to share this configuration with others.

Run it either directly without any shared volumes with
```
sudo docker run -d --link mongodb:db --name ceres-collector kordano/ceres-collector
```
or define a shared volume in `opt/deploy-ceres` for backup and log-files and run it
```
sudo sh ./opt/deploy-ceres
```


## License

Copyright © 2015 Konrad Kühne

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
