import pandas as pd
import logging
import os

def code_mapper(f_content, idx):
    
    '''
    to extract the key-value pair of immigration data from label description
    '''
    
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    
    return dic

def convert_mapping_to_pdf(mapping, join_column_name, target_column_name):
    
    '''
    to convert dictionary of mapping code to pandas dataframe
    '''
    
    return pd.DataFrame.from_dict(mapping, orient='index').reset_index().rename({'index' :  join_column_name, 0 : target_column_name},axis=1)

def main():
    
    label_description_file = '/home/workspace/data/I94_SAS_Labels_Descriptions.SAS'
    
    if not os.path.exists('/home/workspace/output/mapping_data'):
        os.mkdir('/home/workspace/output/mapping_data')

    with open(label_description_file) as f:
        
        logging.info('open sas file and extract key-value pair from mapping')
        f_content = f.read()
        f_content = f_content.replace('\t', '')
        i94cit_res = code_mapper(f_content, "i94cntyl")
        i94port = code_mapper(f_content, "i94prtl")
        i94mode = code_mapper(f_content, "i94model")
        i94addr = code_mapper(f_content, "i94addrl")
        i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}
        
        logging.info('convert to pandas dataframe')
        city_code = convert_mapping_to_pdf(i94cit_res, join_column_name = 'i94cit', target_column_name = 'born_country')
        residence_code = convert_mapping_to_pdf(i94cit_res, join_column_name = 'i94res', target_column_name = 'residence_country')
        port_code = convert_mapping_to_pdf(i94port, join_column_name = 'i94port', target_column_name = 'immigration_port')
        mode_code = convert_mapping_to_pdf(i94mode, join_column_name = 'i94mode', target_column_name = 'transportation')
        addr_code = convert_mapping_to_pdf(i94addr, join_column_name = 'i94addr', target_column_name = 'state')
        visa_code = convert_mapping_to_pdf(i94visa, join_column_name = 'i94visa', target_column_name = 'visa_code')
        
        logging.info('saving mapping files in csv format')
        city_code.to_csv('/home/workspace/output/mapping_data/city_code.csv', index=False, header=True)
        residence_code.to_csv('/home/workspace/output/mapping_data/residence_code.csv', index=False, header=True)
        port_code.to_csv('/home/workspace/output/mapping_data/port_code.csv', index=False, header=True)
        mode_code.to_csv('/home/workspace/output/mapping_data/mode_code.csv', index=False, header=True)
        addr_code.to_csv('/home/workspace/output/mapping_data/addr_code.csv', index=False, header=True)
        visa_code.to_csv('/home/workspace/output/mapping_data/visa_code.csv', index=False, header=True)
    
if __name__ == "__main__":
    
    main()
    