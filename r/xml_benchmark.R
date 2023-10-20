library(xml2)
library(methods)
library(parallel)
library(tibble)
library(here)
library(tidyverse)

path_to_data = paste0(here(), '/data/')


parse_person = function(person){
    ns=c(irs="http://www.irs.gov/efile")
    name = xml_find_first(person, ".//irs:BusinessName/irs:BusinessNameLine1Txt", ns=ns) %>% xml_text() 
    comp = xml_find_first(person, ".//irs:ReportableCompFromOrgAmt", ns=ns) %>% xml_text() 
    title = xml_find_first(person, ".//irs:TitleTxt", ns=ns) %>% xml_text() 
    other = xml_find_first(person, ".//irs:OtherCompensationAmt", ns=ns) %>% xml_text()
    alt_name = xml_find_first(person, ".//irs:PersonNm", ns=ns) %>% xml_text()
    return(c(name=name, comp=comp, title=title, other=other, alt_name=alt_name))
}
parse_irs_xml = function(filename){
    ns=c(irs="http://www.irs.gov/efile")
    tryCatch(
        expr={
            my_xml = read_xml(paste0(path_to_data, 'xml/', filename))
            xml_find_first(my_xml ,"//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt", ns=ns) %>%
                xml_text() -> institution_name
            xml_find_all(my_xml ,"//irs:Form990PartVIISectionAGrp", ns=ns) -> people
            out_list = lapply(people, parse_person)
            comp_tibble = bind_rows(out_list)
            comp_tibble = comp_tibble %>% mutate(total_comp = as.numeric(comp) + as.numeric(other))
            comp_tibble['institution'] = institution_name
            comp_tibble['filename'] = filename
            return(comp_tibble)
        },
        error=function(e){return(tibble())}
    )
}

files = list.files(paste0(path_to_data, 'xml/'))
s = system.time({
    parsed = lapply(files, parse_irs_xml)
})
print(s)


library(parallel)
s = system.time({
parsed = mclapply(files, parse_irs_xml, mc.cores=9)
})
print(s)

df = bind_rows(parsed)
write_csv(, paste0(here(), '/r', '/nonprofit_salaries.csv'))
df %>% summarise(sum(total_comp))
# 48538150203
