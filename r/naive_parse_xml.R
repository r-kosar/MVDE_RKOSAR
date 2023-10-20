library(tidyverse)
library(XML)
library(methods)
path_to_data = '~/Desktop/MVDE_RKOSAR/data/'
data = read_csv(paste0(path_to_data, 'index_2023.csv'),
                col_types=str_dup('c', 9))
data %>%
  filter(RETURN_TYPE=='990') %>%
  filter(str_detect(TAXPAYER_NAME, regex("Rice University", ignore_case = TRUE))) %>%
  pull(OBJECT_ID)

rice_xml = read_xml(paste0(path_to_data, "xml/202321259349302052_public.xml"))

# assumes every persone has name and comp and title
rice_xml %>%
  xml_ns_strip()
xml_find_all(rice_xml ,"//Form990PartVIISectionAGrp/BusinessName/BusinessNameLine1Txt") %>%
  xml_text() -> name
xml_find_all(rice_xml ,"//Form990PartVIISectionAGrp/ReportableCompFromOrgAmt") %>%
  xml_text() -> comp
xml_find_all(rice_xml ,"//Form990PartVIISectionAGrp/TitleTxt") %>%
  xml_text() -> title
xml_find_all(rice_xml ,"//Form990PartVIISectionAGrp/OtherCompensationAmt") %>%
  xml_text() -> other

comp_tibble = tibble(name=name, title=title, total_comp=as.numeric(comp) + as.numeric(other))
comp_tibble %>% arrange(desc(total_comp)) %>% write_clip()

parse_irs_xml = function(filename){
  tryCatch(
    expr={
      my_xml = read_xml(paste0(path_to_data, 'xml/', filename))
      my_xml %>%
        xml_ns_strip()
      xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/BusinessName/BusinessNameLine1Txt") %>%
        xml_text() -> name
      xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/ReportableCompFromOrgAmt") %>%
        xml_text() -> comp
      xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/TitleTxt") %>%
        xml_text() -> title
      xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/OtherCompensationAmt") %>%
        xml_text() -> other
      comp_tibble = tibble(name=name, title=title, total_comp=as.numeric(comp) + as.numeric(other))
      return(comp_tibble)
    },
    error=function(e){return(tibble())}
  )
}

files = list.files(paste0(path_to_data, 'xml/'))
s = system.time({
parsed = lapply(files, parse_irs_xml)
})
df = bind_rows(parsed)


parse_irs_xml('20239349302052_public.xml')

filename='20239349302052_public.xml'
my_xml = read_xml(paste0(path_to_data, 'xml/', filename))
my_xml %>%
  xml_ns_strip()
xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/BusinessName/BusinessNameLine1Txt") %>%
  xml_text() -> name
xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/ReportableCompFromOrgAmt") %>%
  xml_text() -> comp
xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/TitleTxt") %>%
  xml_text() -> title
xml_find_all(my_xml ,"//Form990PartVIISectionAGrp/OtherCompensationAmt") %>%
  xml_text() -> other
comp_tibble = tibble(name=name, title=title, total_comp=as.numeric(comp) + as.numeric(other))
