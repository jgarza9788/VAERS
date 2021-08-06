
import pandas as pd
import numpy as np
import os
import sys
import re
from matplotlib import pyplot as plt
import json
from collections import Counter


DIR = os.path.dirname(os.path.realpath(__file__))
datapath = os.path.join(DIR,'AllVAERSDataCSVS')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_file_list(path):
    """
    returns a list of all the files in the given path/directory
    """
    return [ os.path.join(path,f) for f in os.listdir(path)]

# def compile_data_all_VAERSDATA():
#     """
#     compiles 
#     """
#     csv_files = get_file_list(datapath)
#     data_files = [f for f in csv_files if re.search(r'\d{4}VAERSDATA',f)]
#     df = pd.DataFrame()
#     for i,f in enumerate(data_files):
#         print(i,f)
#         df0 = pd.read_csv(f,encoding='cp1252')
#         df = pd.concat([df,df0])
#     return df

def compile_files(directory,files):
    """
    compiles/adds/unions multiple csv files together and returns a dataframe
    """
    df = pd.DataFrame()
    for i,f in enumerate(files):
        # print(i,f)
        df0 = pd.read_csv(os.path.join(directory,f),encoding='cp1252',low_memory=False)
        df = pd.concat([df,df0])
    return df


def load_json(path_to_file):
    """
    loads json from file
    """
    with open(path_to_file, 'r') as f:
        return json.load(f)


def save_json(path_to_file,data):
    """
    writes dict/json to file
    """
    with open(path_to_file, 'w') as f:
        json.dump(data, f)

def has_covid(text):
    """
    returns 1 or 0 if text has \'COVID\' in it
    """
    if re.search('COVID',text.upper()):
        return 1
    else:
        return 0

def process_to_one_file():
    """
    processes VAERS data from 2020 and 2021, creates all_data.json, and returns a dataframe
    """
    print('process to one file\n\tthis might take a while...go get a drink☕🍷🍸🍹🍶🍺\n')
    df_data = compile_files(datapath,['2020VAERSDATA.csv','2021VAERSDATA.csv'])
    df_vax = compile_files(datapath,['2020VAERSVAX.csv','2021VAERSVAX.csv'])
    df_sym = compile_files(datapath,['2020VAERSSYMPTOMS.csv','2021VAERSSYMPTOMS.csv'])

    print("""
    symptoms are contained in columns (up to 5 symptoms per event)
    we must transform these symptoms into a single list for each event
    """)
    print('dedup-ing Symptoms')
    vid = list(df_sym['VAERS_ID'].unique())

    idf_sym = []
    for index,v in enumerate(vid):
        if index%100 == 0:
            print('{:.2f}'.format(index/len(vid)))
        temp = df_sym[df_sym['VAERS_ID'] == v]
        temp = temp.to_dict(orient='records')
        syms = []
        for t in temp:
            syms.append(t['SYMPTOM1'])
            syms.append(t['SYMPTOM2'])
            syms.append(t['SYMPTOM3'])
            syms.append(t['SYMPTOM4'])
            syms.append(t['SYMPTOM5'])
        idf_sym.append({'VAERS_ID':v,'SYMPTOMS':syms})
    df_sym = pd.DataFrame(idf_sym)

    print('merge data')
    df = pd.merge(df_data,df_vax,how='outer',on='VAERS_ID')
    df = df.drop_duplicates(ignore_index = True)
    df = pd.merge(df,df_sym,how='outer',on='VAERS_ID')

    df.reset_index()

    # creating a new column depending if this is the covid vaccine or not
    df['COVID_VAX'] = df['VAX_TYPE'].apply(has_covid)
    df = df[df['COVID_VAX'] == 1]
    # print(len(df))

    print('all columns\n',df.columns)
    print('top 50\n',df.head(50))

    f0 = os.path.join(datapath,'all_data.csv') #not really needed...but some people might like a csv
    f1 = os.path.join(datapath,'all_data.json')
    df.to_csv(f0)
    save_json(f1,df.to_dict(orient='records'))
    print('saved: ',f0)
    print('saved: ',f1)

    return df


def break_down_2(_df,column):
    """
    shows what values there are for a given column (with counts and percent)
    """
    print('\nbreak down of {0}'.format(column))
    df = pd.DataFrame(_df[column])
    df = df.fillna('nan')

    print('column'.ljust(10),'\t','value'.ljust(10),'\t','count'.ljust(10),'\t','percent'.ljust(10))

    l = list(df[column].unique())
    for i in l:
        df0 = df[df[column]==i]
        print(column.ljust(10),'\t',str(i).ljust(10),'\t',str(len(df0)).ljust(10),'\t','{:.2f}'.format((len(df0)/len(df))*100).ljust(10))

def process_symptoms_to_list(df):
    """
    returns a list of symptoms for the dataframe
    """
    s = df['SYMPTOMS'].to_list()
    l = []
    for i in s:
        try:
            for j in i:
                if str(j) == 'nan':
                    pass
                else:
                    l.append(str(j).upper())
        except:
            pass
    return l

def break_down_3(_df,column,buckets,message=''):
    """
    breaks a column down into buckets/bins
    """
    print('\n\n',message,'\ncolumn: ',column, '\n','buckets: ', buckets)
    
    df = pd.DataFrame(_df[column])
    df['bucket'] = pd.cut(df[column], bins=buckets)
    df = df.groupby(by='bucket').count()
    df['percent'] = (df[column]/df[column].sum())*100
    df['percent'] = df['percent'].round(2)
    print(df)

def symptom_list(df,top=100):
    """
    displays a list of the most popular symptoms
    note: symptoms might be medical jargon or plain english
    i.e. \"RASH\",\"ERYTHEMA\", and \"ITCHY RED SKIN\" would be reported as different items (for now)
    """

    print(
"""
note: symptoms might be medical jargon or plain english
i.e. \"RASH\",\"ERYTHEMA\", and \"ITCHY RED SKIN\" would be reported as different items
"""
    )

    symptoms =  process_symptoms_to_list(df)
    symp_count = len(symptoms)
    symptoms = Counter(symptoms)
    symptoms = symptoms.most_common()
    # print(symptoms)

    print('\ntop {top} symptoms'.format(top=top))
    cs = [4,24,12,12]
    print('#'.ljust(cs[0]),'symptom'.ljust(cs[1]),'value'.ljust(cs[2]),'percent'.ljust(cs[3]))
    for index,i in enumerate(symptoms[0:top]):
        print(
            str(index).ljust(cs[0]),
            str(i[0][0:cs[1]]).ljust(cs[1]),
            str(i[1]).ljust(cs[2]),
            '{:.2f}'.format((i[1]/symp_count)*100).ljust(cs[3])
        )
    
    return symptoms

def get_data():
    """
    gets the data and returns a dataframe
    """

    all_data = os.path.join(datapath,'all_data.json')

    if os.path.isfile(all_data):
        print('loading all_data.json')
        df = load_json(all_data)
        df = pd.DataFrame(df)
    else:
        print('processing the 2020-2021 files')
        print("""
        .../VAERS/AllVAERSDataCSVS/  
            2021VAERSVAX.csv  
            2021VAERSSYMPTOMS.csv  
            2021VAERSDATA.csv  
            2020VAERSVAX.csv  
            2020VAERSSYMPTOMS.csv  
            2020VAERSDATA.csv 
        """)
        df = process_to_one_file()

    print(df.columns)

    return df


def breakdowns(df):
    """
    breaks down the data
    """
    ddf = df[df['DIED']=='Y']

    break_down_3(df,'AGE_YRS',[0,15,25,35,45,55,65,75,85,500])
    break_down_3(ddf,'AGE_YRS',[0,15,25,35,45,55,65,75,85,500],message='***deaths only***')

    break_down_3(df,'NUMDAYS',[0,10,20,30,40,50,60])
    break_down_3(ddf,'NUMDAYS',[0,10,20,30,40,50,60],message='***deaths only***')

    break_down_2(df,'DIED')
    break_down_2(df,'ER_VISIT')
    break_down_2(df,'L_THREAT')
    break_down_2(df,'RECOVD')



def percentages_2(vaers_min,vaers_max,vaers_label,vax_count,vax_label):
    """
    prints the percents
    """
    print(
        '\n( {vaers_label} / {vax_label} ) * 100\n'.format(
            vaers_label=vaers_label,
            vax_label=vax_label
            ),
        '{:.2f}'.format((vaers_min/vax_count)*100),
        ' - ',
        '{:.2f}'.format((vaers_max/vax_count)*100),
        '\n'
        )        

def percentages(df,vax_count,vax_label,vaers_min_adj=80,vaers_max_adj=120):
    """
    calculates and prints the percentages
    """

    print("""
VAERS only contains reported data and     
\'...fewer than 1% of vaccine adverse events are reported.\'
resources:
https://digital.ahrq.gov/sites/default/files/docs/publication/r18hs017045-lazarus-final-report-2011.pdf (page 6)
    """)

    print('therfore, will be multiplying the VAERS counts by {0} and {1}'.format(vaers_min_adj,vaers_max_adj))
    print('thus providing min and max percentages.')

    VAERS_count = len(df)
    VAERS_death_count = len(df[df['DIED']=='Y'])
    VAERS_nrecovd_count = len(df[df['RECOVD']=='N'])

    vc_min = VAERS_count * vaers_min_adj
    vc_max = VAERS_count * vaers_max_adj

    vdc_min = VAERS_death_count * vaers_min_adj
    vdc_max = VAERS_death_count * vaers_max_adj

    vnr_min = VAERS_nrecovd_count * vaers_min_adj
    vnr_max = VAERS_nrecovd_count * vaers_max_adj

    percentages_2(
        VAERS_count * vaers_min_adj,
        VAERS_count * vaers_max_adj,
        'adverse reaction',
        vax_count,
        vax_label
        )
    percentages_2(
        VAERS_death_count * vaers_min_adj,
        VAERS_death_count * vaers_max_adj,
        'adverse death',
        vax_count,
        vax_label
        )
    percentages_2(
        VAERS_nrecovd_count * vaers_min_adj,
        VAERS_nrecovd_count * vaers_max_adj,
        'no recovery',
        vax_count,
        vax_label)


    

def main():
    """
    this will do all the things
    """

    #this might not work on linux or macOS
    try:
        os.system('cls')
    except:
        pass

    #settings
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    df = get_data()

    print('\n\n--------------------------------\n\n')

    symptoms = symptom_list(df,top=100)

    print('\n\n--------------------------------\n\n')

    breakdowns(df)

    print('\n\n--------------------------------\n\n')

    full_VAX = 165*10**6 #from google 8/3/2021
    print('according to a quick google search (on 8/3/2021) {0:,} have had two doses of the vaccine (full_vax)'.format(full_VAX))
    percentages(df,full_VAX,'full_vax',80,120)

    print('\n\n--------------------------------\n\n')

    half_VAX = 191*10**6 #from google 8/3/2021
    print('according to a quick google search (on 8/3/2021) {0:,} have had (at least) one dose of the vaccine (half_vax)'.format(half_VAX))
    percentages(df,half_VAX,'half_vax',80,120)


if __name__ == '__main__':

    main()
