
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
    return [ os.path.join(path,f) for f in os.listdir(path)]

def compile_data_all_VAERSDATA():
    csv_files = get_file_list(datapath)
    data_files = [f for f in csv_files if re.search(r'\d{4}VAERSDATA',f)]
    df = pd.DataFrame()
    for i,f in enumerate(data_files):
        print(i,f)
        df0 = pd.read_csv(f,encoding='cp1252')
        df = pd.concat([df,df0])
    return df

def compile_files(directory,files):
    df = pd.DataFrame()
    for i,f in enumerate(files):
        # print(i,f)
        df0 = pd.read_csv(os.path.join(directory,f),encoding='cp1252',low_memory=False)
        df = pd.concat([df,df0])
    return df


def load_json(path_to_file):
    with open(path_to_file, 'r') as f:
        return json.load(f)


def save_json(path_to_file,data):
    with open(path_to_file, 'w') as f:
        json.dump(data, f)

def has_covid(text):
    if re.search('COVID',text.upper()):
        return 1
    else:
        return 0

def process_to_one_file():
    print('process to one file\n\n')
    df_data = compile_files(datapath,['2020VAERSDATA.csv','2021VAERSDATA.csv'])
    df_vax = compile_files(datapath,['2020VAERSVAX.csv','2021VAERSVAX.csv'])
    df_sym = compile_files(datapath,['2020VAERSSYMPTOMS.csv','2021VAERSSYMPTOMS.csv'])

    print('dedup-ing Symptoms')
    vid = list(df_sym['VAERS_ID'].unique())

    idf_sym = []
    # for index,v in enumerate(vid[0:100]):
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
    # df = df.head(25)

    # print(len(df))
    df['COVID_VAX'] = df['VAX_TYPE'].apply(has_covid)
    df = df[df['COVID_VAX'] == 1]
    # print(len(df))

    print('all columns\n',df.columns)
    print('top 50\n',df.head(50))

    f0 = os.path.join(datapath,'all_data.csv')
    f1 = os.path.join(datapath,'all_data.json')
    df.to_csv(f0)
    save_json(f1,df.to_dict(orient='records'))
    print('saved: ',f0)
    print('saved: ',f1)

    return df


def break_down(_df,column,q=[0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1]):
    print('\nbreak down of {0}'.format(column))
    df = pd.DataFrame(_df[column])
    bucket_column = column + '_bucket'
    df[bucket_column] = pd.qcut(
            df[column],
            q=q,
            labels=False,
            precision=0,
            duplicates='drop',
        )

    # print(df.head())

    dmin = df.groupby(by=bucket_column).min()
    dmin = dmin.rename(columns={column: column + '_min'})
    dmax = df.groupby(by=bucket_column).max()
    dmax = dmax.rename(columns={column: column + '_max'})
    dcount = df.groupby(by=bucket_column).count()
    dcount = dcount.rename(columns={column: column + '_count'})

    df0 = pd.merge(dmin,dmax,how='outer',on=bucket_column)
    df0 = pd.merge(df0,dcount,how='outer',on=bucket_column)

    df0['percent'] = (df0[column + '_count']/df0[column + '_count'].sum()) * 100

    print(df0)

def break_down_2(_df,column):
    print('\nbreak down of {0}'.format(column))
    df = pd.DataFrame(_df[column])
    df = df.fillna('nan')

    print('column'.ljust(10),'\t','value'.ljust(10),'\t','count'.ljust(10),'\t','percent'.ljust(10))

    l = list(df[column].unique())
    for i in l:
        df0 = df[df[column]==i]
        print(column.ljust(10),'\t',str(i).ljust(10),'\t',str(len(df0)).ljust(10),'\t','{:.2f}'.format((len(df0)/len(df))*100).ljust(10))


def process_symptoms_to_list(df):
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
    print('\n\n',message,'\ncolumn: ',column, '\n','buckets: ', buckets)
    
    df = pd.DataFrame(_df[column])
    df['bucket'] = pd.cut(df[column], bins=buckets)
    df = df.groupby(by='bucket').count()
    df['percent'] = (df[column]/df[column].sum())*100
    df['percent'] = df['percent'].round(2)
    print(df)



if __name__ == '__main__':
    #settings
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_colwidth', 0)

    # df = process_to_one_file()

    df = load_json(os.path.join(datapath,'all_data.json'))
    df = pd.DataFrame(df)
    print(df.columns)


    symptoms =  process_symptoms_to_list(df)
    symp_count = len(symptoms)
    symptoms = Counter(symptoms)
    symptoms = symptoms.most_common()
    # print(symptoms)

    print('\ntop 100 symptoms')
    cs = [4,24,12,12]
    print('#'.ljust(cs[0]),'symptom'.ljust(cs[1]),'value'.ljust(cs[2]),'percent'.ljust(cs[3]))
    for index,i in enumerate(symptoms[0:100]):
        print(
            str(index).ljust(cs[0]),
            str(i[0][0:cs[1]]).ljust(cs[1]),
            str(i[1]).ljust(cs[2]),
            '{:.2f}'.format((i[1]/symp_count)*100).ljust(cs[3])
        )

    ddf = df[df['DIED']=='Y']

    print('\n\n')
    break_down_3(df,'AGE_YRS',[0,15,25,35,45,55,65,75,85,500])
    break_down_3(ddf,'AGE_YRS',[0,15,25,35,45,55,65,75,85,500],message='deaths only')

    break_down_3(df,'NUMDAYS',[0,10,20,30,40,50,60])
    break_down_3(ddf,'NUMDAYS',[0,10,20,30,40,50,60],message='deaths only')

    break_down_2(df,'DIED')
    break_down_2(df,'ER_VISIT')
    break_down_2(df,'L_THREAT')
    break_down_2(df,'RECOVD')

    full_VAX = 165*10**6 #from google 8/3/2021
    half_VAX = 191*10**6 #from google 8/3/2021
    VAERS_count = len(df)
    VAERS_death_count = len(df[df['DIED']=='Y'])
    VAERS_nrecovd_count = len(df[df['RECOVD']=='N'])

    vc_min = VAERS_count * 80
    vc_max = VAERS_count * 120

    vdc_min = VAERS_death_count * 80
    vdc_max = VAERS_death_count * 120

    vnr_min = VAERS_nrecovd_count * 80
    vnr_max = VAERS_nrecovd_count * 120

    print('\n\n--------------------------------\n\n')

    print('\nadverse rections / full vax\n',
        '{:.2f}'.format((vc_min/full_VAX)*100),
        ' - ',
        '{:.2f}'.format((vc_max/full_VAX)*100))
    print('\nadverse death / full vax\n',
        '{:.2f}'.format((vdc_min/full_VAX)*100),
        ' - ',
        '{:.2f}'.format((vdc_max/full_VAX)*100))
    print('\nno recovery / full vax\n',
        '{:.2f}'.format((vnr_min/full_VAX)*100),
        ' - ',
        '{:.2f}'.format((vnr_max/full_VAX)*100))


    print('\n\n--------------------------------\n\n')

    print('\nadverse rections / half vax\n',
        '{:.2f}'.format((vc_min/half_VAX)*100),
        ' - ',
        '{:.2f}'.format((vc_max/half_VAX)*100))
    print('\nadverse death / half vax\n',
        '{:.2f}'.format((vdc_min/half_VAX)*100),
        ' - ',
        '{:.2f}'.format((vdc_max/half_VAX)*100))
    print('\nno recovery / half vax\n',
        '{:.2f}'.format((vnr_min/half_VAX)*100),
        ' - ',
        '{:.2f}'.format((vnr_max/half_VAX)*100))


