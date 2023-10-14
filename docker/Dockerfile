# Use a base image that includes R and Python
FROM rocker/r-ver:4.1.1

# Install R dependencies
RUN R -e "install.packages('tidyverse', dependencies=TRUE)"
RUN R -e "install.packages('SparkR', dependencies=TRUE)"

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install pandas numpy ray ipython

# Set the working directory
WORKDIR /app
