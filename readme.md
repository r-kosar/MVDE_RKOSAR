# What's in this repo?
This repo supports a presentation I gave to the fall 2023 Multivariate Density Estimation course at Rice university. It's about ssome softwaretools I wish I'd known more about in grad-school including:

* spark
* docker
* r packaging
* ray
* git/github
* sql
* automated testing

# How does the repo work?
I only covered half the topics as I split it with another alumnus. The repo is an example of using spark, docker, and ray to explore the tax filings of nonprofits.  Here are the steps:
1. Run `bash download_2023.sh` to download the 2023 tax filings.
2. Build the docker `docker build -t my_docker ./docker`
3. Start the docker `docker run  --network="host" -v ./data:/app -it my_docker` 