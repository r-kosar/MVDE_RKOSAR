import os
import pandas as pd
from lxml import etree
import ray
import time
import numpy as np


path_to_data = "../data"  # Replace with your actual data path
def parse_irs_xml(filename):
    def if_none(obj, alt):
        if obj is not None:
            return(obj.text)
        else:
            return(alt)
        
    def parse_person(person):
      name = if_none(person.find(".//irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns), 'Unknown')
      comp = if_none(person.find(".//irs:ReportableCompFromOrgAmt", namespaces=ns), np.NaN)
      title = if_none(person.find(".//irs:TitleTxt", namespaces=ns), 'Unknown')
      other = if_none(person.find(".//irs:OtherCompensationAmt", namespaces=ns), np.NaN)
      alt_name = if_none(person.find(".//irs:PersonNm", namespaces=ns), 'Unknown')
      return({'name': name,
              'comp': comp,
              'title': title,
              'other': other,
              'alt_name': alt_name})
    
    try:
        with open(os.path.join(path_to_data, 'xml', filename), 'rb') as file:
            tree = etree.parse(file)
            
        ns =  {'irs':"http://www.irs.gov/efile"}
        
        # Extract data
        institution_name = tree.xpath("//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns)[0].text
        people = [p for p in tree.xpath("//irs:Form990PartVIISectionAGrp", namespaces=ns)]
        people_records = [parse_person(p) for p in people]
        
        # Create a DataFrame
        comp_df = pd.DataFrame(people_records)
        comp_df['total_comp'] = pd.to_numeric(comp_df['comp'], errors='coerce') + pd.to_numeric(comp_df['other'], errors='coerce')
        comp_df['institution'] = institution_name
        comp_df['filename'] = filename
        return comp_df
    except Exception as e:
        return pd.DataFrame()

files = os.listdir(os.path.join(path_to_data, 'xml'))
start_time = time.time()
parsed = [parse_irs_xml(filename) for filename in files]
end_time = time.time()
elapsed_time = end_time - start_time
print(f'naive: {elapsed_time}')
# 352


from joblib import Parallel, delayed
start_time = time.time()
parsed = Parallel(n_jobs=9)(delayed(parse_irs_xml)(f) for f in files)
end_time = time.time()
elapsed_time = end_time - start_time
print(f'joblib: {elapsed_time}')
# 67

ray.init(num_cpus=9)
@ray.remote
def parse_irs_xml_ray(filename):
    def if_none(obj, alt):
        if obj is not None:
            return(obj.text)
        else:
            return(alt)
        
    def parse_person(person):
      name = if_none(person.find(".//irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns), 'Unknown')
      comp = if_none(person.find(".//irs:ReportableCompFromOrgAmt", namespaces=ns), np.NaN)
      title = if_none(person.find(".//irs:TitleTxt", namespaces=ns), 'Unknown')
      other = if_none(person.find(".//irs:OtherCompensationAmt", namespaces=ns), np.NaN)
      alt_name = if_none(person.find(".//irs:PersonNm", namespaces=ns), 'Unknown')
      return({'name': name,
              'comp': comp,
              'title': title,
              'other': other,
              'alt_name': alt_name})
    
    try:
        with open(os.path.join(path_to_data, 'xml', filename), 'rb') as file:
            tree = etree.parse(file)
            
        ns =  {'irs':"http://www.irs.gov/efile"}
        
        # Extract data
        institution_name = tree.xpath("//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns)[0].text
        people = [p for p in tree.xpath("//irs:Form990PartVIISectionAGrp", namespaces=ns)]
        people_records = [parse_person(p) for p in people]
        
        # Create a DataFrame
        comp_df = pd.DataFrame(people_records)
        comp_df['total_comp'] = pd.to_numeric(comp_df['comp'], errors='coerce') + pd.to_numeric(comp_df['other'], errors='coerce')
        comp_df['institution'] = institution_name
        comp_df['filename'] = filename
        return comp_df
    except Exception as e:
        return pd.DataFrame()

files = os.listdir(os.path.join(path_to_data, 'xml'))
start_time = time.time()
parsed = ray.get([parse_irs_xml_ray.remote(f) for f in files])
end_time = time.time()
elapsed_time = end_time - start_time
print(f'ray: {elapsed_time}')
# 125



@ray.remote
def parse_irs_xml_ray_chunked(file_list):
    def if_none(obj, alt):
        if obj is not None:
            return(obj.text)
        else:
            return(alt)
        
    def parse_person(person):
      name = if_none(person.find(".//irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns), 'Unknown')
      comp = if_none(person.find(".//irs:ReportableCompFromOrgAmt", namespaces=ns), np.NaN)
      title = if_none(person.find(".//irs:TitleTxt", namespaces=ns), 'Unknown')
      other = if_none(person.find(".//irs:OtherCompensationAmt", namespaces=ns), np.NaN)
      alt_name = if_none(person.find(".//irs:PersonNm", namespaces=ns), 'Unknown')
      return({'name': name,
              'comp': comp,
              'title': title,
              'other': other,
              'alt_name': alt_name})
  
    out = []
    for filename in file_list:
        try:
            with open(os.path.join(path_to_data, 'xml', filename), 'rb') as file:
                tree = etree.parse(file)
            
            ns =  {'irs':"http://www.irs.gov/efile"}

            # Extract data
            institution_name = tree.xpath("//irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt", namespaces=ns)[0].text
            people = [p for p in tree.xpath("//irs:Form990PartVIISectionAGrp", namespaces=ns)]
            people_records = [parse_person(p) for p in people]

            # Create a DataFrame
            comp_df = pd.DataFrame(people_records)
            comp_df['total_comp'] = pd.to_numeric(comp_df['comp'], errors='coerce') + pd.to_numeric(comp_df['other'], errors='coerce')
            comp_df['institution'] = institution_name
            comp_df['filename'] = filename

            out.append(comp_df)
        except Exception as e:
            out.append(pd.DataFrame())
    return(out)

def chunk_list(input_list, n):
    return [input_list[i:i + n] for i in range(0, len(input_list), n)]

def flatten_list(nested_list):
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))
        else:
            flattened.append(item)
    return flattened

start_time = time.time()
chunked = chunk_list(files, 64)
parsed = flatten_list(ray.get([parse_irs_xml_ray_chunked.remote(c) for c in chunked]))
end_time = time.time()
elapsed_time = end_time - start_time
print(f'chunked ray: {elapsed_time}')
# 74

df.to_csv('/Users/rspok/Desktop/MVDE_RKOSAR/python/nonprofit_salaries.csv', index=False)
np.sum(df['total_comp'])
# sanity check with R implementation
# 48538150203.0
