library(tidyverse)
library(sparklyr)
spark_home='/opt/spark-3.4.1-bin-hadoop3'
config <- spark_config()
config$sparklyr.connect.enablehivesupport = FALSE
#config$spark.executor.memory <- "1G"

# this command may take a minute
# run docker ps followed by
# docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <spark master container id or name>
# to get the master address

sc <- spark_connect(spark_home=spark_home,
                    master = "spark://spark:7077",
                    config=config)
data = spark_read_csv(sc, name = "salary_data",
                      path = "nonprofit_salaries.csv",
                      header = TRUE, delimiter = ",")


fix_names = data %>%
    mutate(name=case_when(name=='NA' ~ NA,
                          TRUE ~ name)) %>%
    mutate(alt_name=case_when(alt_name=='NA' ~ NA,
                              TRUE ~ alt_name)) %>%
    mutate(derived_name = coalesce(name, alt_name)) %>%
    mutate(total_comp = as.numeric(total_comp))

fix_names  %>%
    select(derived_name, title, institution, total_comp) %>%
    arrange(desc(total_comp)) %>%
    filter(row_number() <= 20) %>%
    print(width=100, n=12)

fix_names %>%
    filter(total_comp > 0) %>%
    group_by(derived_name, total_comp) %>%
    summarize(count = n()) %>%
    mutate(total_total_comp = count * total_comp) %>%
    arrange(desc(total_total_comp)) %>%
    print(n=25)

fix_names %>%
    filter(derived_name == 'LLOYD H DEAN' &
           total_comp == 9642249,) %>%
    pull(institution)

fix_names %>%
    filter(derived_name == 'CRAIG A CORDOLA FACHE' &
           total_comp == 1052538) %>%
    pull(institution)

