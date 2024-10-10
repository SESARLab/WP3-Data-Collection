# WP-Data-Collection
Repository for all the WP3 data collectiors

## Social Crawler
#### Requirements
* Docker must be installed
* **HDFS must be up, running and reacheable**
### Get Started

#### API key and HDFS configuration


before starting you need to enter your credentials (API Keys and similar) in the `app-config.yml` file.

A template is available in the [`app-config.yml.sample`](CounteR/app-config.yml.sample) file

If you don't have HFDS available, you can also deploy it via docker ([here a docker-compose example](https://github.com/big-data-europe/docker-hadoop/blob/master/docker-compose.yml))

#### Start the Crawler
In order to start the crawler you'll need to run these commands

```
git clone <>
docker-compose build
docker-compose up
```

This will bring up the system which is composed by:
| Service  | Port Exposed|
| ---------------------|----|
| Asynchronous Api     |5001|
| The Synchronous Api  |8000|
| Flower               |7000|
| Redis                | N/A|
# WP3-Data-Collection
