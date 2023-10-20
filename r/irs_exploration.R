library(tidyverse)
library(sparklyr)


# connecting takes a while
sc = spark_connect(spark_home = spark_install_find(version="3.5.0")$sparkVersionDir, 
                   master = "spark://localhost:7077", 
                   config = list(sparklyr.connect.packages =
                                     "com.databricks:spark-xml_2.11:0.5.0"))

spark_read_source(sc, "irs_xml", "./xml", "xml")
