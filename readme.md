# What's in this repo?
This repo supports a presentation I gave to the fall 2023 Multivariate Density Estimation course at Rice university. It's about some software tools I wish I'd known more about in grad-school including:

* spark
* docker
* r packaging
* ray
* git/github
* sql
* automated testing

# How does the repo work?
I only covered half the topics as I split it with another alumnus. The repo is an example of using spark, docker, and ray to explore the tax filings of nonprofits.  Here are the main pieces:

# Parsing the data
1. Run `bash download_2023.sh` to download the 2023 tax filings.
2. Use either of python/parse_irs_xml.py or r/xml_benchmark.R to parse the data.
3. The point of these files is to show that there are low-effort ways to greatly speed up parsing, not to be exemplary parsing code.

# Further analysis using spark
## Build an analysis docker
```docker build --rm -t my_docker ./docker```

## Launch a local standalone spark cluster and the analysis image
```docker-compose spark/docker-compose.yml up```

## Start an analysis shell
```docker exec -it spark-rclient-1 /bin/bash ```

The data will be in the app directory. Play with the data as desired.  There are some examples in r/spark_exploration.

