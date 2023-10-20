# Use a base image that includes R 
FROM rocker/r2u

# Install Java (OpenJDK 11 in this example)
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set Java environment variables (you can customize as needed)
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# Install R dependencies
RUN R -e "install.packages('tidyverse', dependencies=TRUE)"
# match your spark cluster version
RUN R -e "install.packages('sparklyR', dependencies=TRUE); library(sparklyr);  spark_install('3.5.0')"

# Install Python dependencies
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install pandas numpy ray ipython

# Set the working directory
WORKDIR /app
