# Use a base image that includes R 
FROM rocker/r2u

# Install necessary packages
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk scala wget && \
    apt-get clean

# Download and install Apache Spark 3.4.1
WORKDIR /opt
RUN wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz  && \
    tar -xvzf spark-3.4.1-bin-hadoop3.tgz  && \
    mv spark-3.4.1-bin-hadoop3.tgz spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install R dependencies
RUN R -e "install.packages('tidyverse', dependencies=TRUE)"
# match your spark cluster version
RUN R -e "install.packages('sparklyR', dependencies=TRUE)"

# Set the working directory
WORKDIR /app
