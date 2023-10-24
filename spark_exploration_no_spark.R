library(tidyverse)
data = read_csv("nonprofit_salaries.csv")

fix_names = data  %>%
    mutate(derived_name = coalesce(name, alt_name)) %>%
    mutate(total_comp = as.numeric(total_comp))


fix_names %>%
    filter(derived_name == 'CRAIG A CORDOLA FACHE' &
           total_comp == 1052538) %>%
    pull(filename)
