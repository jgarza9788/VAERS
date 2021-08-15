
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
            if isinstance(t['SYMPTOM1'],str):
                syms.append(t['SYMPTOM1'])
            if isinstance(t['SYMPTOM2'],str):
                syms.append(t['SYMPTOM2'])
            if isinstance(t['SYMPTOM3'],str):
                syms.append(t['SYMPTOM3'])
            if isinstance(t['SYMPTOM4'],str):
                syms.append(t['SYMPTOM4'])
            if isinstance(t['SYMPTOM5'],str):
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
        print(column.ljust(10),'\t',str(i).ljust(10),'\t',str(len(df0))[0:10].ljust(10),'\t','{:.2f}'.format((len(df0)/len(df))*100).ljust(10))

def process_symptoms_to_list(df,column='SYMPTOMS'):
    """
    returns a list of symptoms for the dataframe
    """
    s = df[column].to_list()
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

def symptom_list(df,print_top=100,column='SYMPTOMS'):
    """
    displays a list of the most popular symptoms
    note: symptoms might be medical jargon or plain english
    i.e. \"RASH\",\"ERYTHEMA\", and \"ITCHY RED SKIN\" would be reported as different items (for now)
    """

    verbose = True
    if print_top == 0:
        verbose = False

    if verbose:
        print(
"""
note: symptoms might be medical jargon or plain english
i.e. \"RASH\",\"ERYTHEMA\", and \"ITCHY RED SKIN\" would be reported as different items
"""
        )

    symptoms =  process_symptoms_to_list(df,column)
    symp_count = len(symptoms)
    symptoms = Counter(symptoms)
    symptoms = symptoms.most_common()
    # print(symptoms)

    if verbose:
        print('\ntop {print_top} symptoms'.format(print_top=print_top))
    cs = [4,24,12,12]

    if verbose:
        print('#'.ljust(cs[0]),'symptom'.ljust(cs[1]),'value'.ljust(cs[2]),'percent'.ljust(cs[3]))
    
    if verbose:
        for index,i in enumerate(symptoms[0:print_top]):
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


def symptom_filter_search(df, search_list):
    data = df.to_dict(orient='records')

    search_list = [i.upper() for i in search_list]
    

    results = []
    for d in data:
        try:
            d['SYMPTOMS'] = [i.upper() for i in d['SYMPTOMS'] if isinstance(i, str)]
        
            symptom_match = list(set(d['SYMPTOMS']) & set(search_list))
            d['SYMPTOMS_MATCH'] = symptom_match
            d['SYMPTOMS_MATCH_LENGTH'] = len(symptom_match)
            if len(symptom_match) > 0:
                results.append(d)
        except:
            pass

    return pd.DataFrame(results)


def print_row(items,column_lengths=[]):
    row = ''
    for index,i in enumerate(items):
        try:
            cl = column_lengths[index]
        except IndexError:
            cl = 20
        row += str(i)[0:cl].ljust(cl)
    print(row)


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

    print('all data: ',len(df))

    print('\n\n--------------------------------\n\n')

    symptoms = symptom_list(df,print_top=100)

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


def women_issues():
    """
    neighbor (steve's wife...unknown name) mentioned symptoms after first dose of vaccine.
    this is for her, but also other women that might be having these issues.
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

    print('all data: ',len(df))
    print(break_down_2(df,'SEX'))
    

    print('\n\n--------------------------------\n\n')
    
    print(
    """
~63% of the people who are vaccinated are women
https://www.statista.com/statistics/1212103/share-of-persons-initiating-covid-vaccinations-by-gender-us-first-month/
    """
    )

    print('\n\n--------------------------------\n\n')

    women_repro_symptoms = [
        'Intermenstrual bleeding',
        'Menopause',
        'Heavy menstrual bleeding',
        'dysmenorrhoea',
        'ABNORMAL UTERINE BLEEDING',
        'MATERNAL EXPOSURE BEFORE PREGNANCY',
        'MENSTRUATION IRREGULAR',
        'Oligomenorrhea',
        'OLIGOMENORRHOEA',
        'POLYMENORRHOEA',
        'MENSTRUAL DISORDER', 
        'OLIGOMENORRHOEA',
        'ANOVULATORY CYCLE',
        'OVULATION DELAYED',
        'BACTERIAL VAGINOSIS',
        'GYNAECOLOGICAL EXAMINATION ABNORMAL',
        'OVARIAN CYST',
        'BIOPSY UTERUS',
        'UTERINE LEIOMYOMA',
        'HOT FLUSH',
        'BREAST TENDERNESS',
        'BREAST SWELLING',
        'BREAST PAIN',
        'VAGINAL HAEMORRHAGE'
        ]
    women_repro_symptoms = [i.upper() for i in women_repro_symptoms]

    df_symptoms = symptom_filter_search(df,women_repro_symptoms)

    # print('the people who have 1 or more women_repro_symptoms')

    #women only
    w_df = df[df['SEX']=='F']
    u_df = df[df['SEX']=='U']
    w_count = len(w_df) + (len(u_df)/2)

    vaxxed = 191*10**6 #from google 8/3/2021 (one or more vaccination)

    women_vaxxed = vaxxed * 0.63


    vaers_ratio = ( len(df_symptoms)/w_count )

    #based on the ratio of repro symptoms and vaers women
    wrs = women_vaxxed * vaers_ratio

    min_wrs = wrs * 0.80
    max_wrs = wrs * 1.20

    # minmin_wrs_percent = (min_wrs/min_F_vaers)*100
    # minmax_wrs_percent = (min_wrs/max_F_vaers)*100
    # maxmax_wrs_percent = (max_wrs/max_F_vaers)*100
    # maxmin_wrs_percent = (max_wrs/min_F_vaers)*100

    cl = [25,5,15]
    print_row(['total vaxxed (1 or more)','','{:,.2f}'.format(vaxxed)],column_lengths=cl)
    print_row(['women vaxxed ~0.63%','','{:,.2f}'.format(women_vaxxed)],column_lengths=cl)
    print_row(['repro sympt / women count','','{:,.4f}'.format(vaers_ratio)],column_lengths=cl)
    print_row(['women w/ repro symptoms','','{:,.2f}'.format(wrs)],column_lengths=cl)
    print_row(['min women w/ repro symptoms','','{:,.2f}'.format(min_wrs)],column_lengths=cl)
    print_row(['min women w/ repro symptoms','','{:,.2f}'.format(max_wrs)],column_lengths=cl)


    print('\n\n--------------------------------\n\n')

    print('most common to least common symptoms and how they compare to all_symptoms')

    all_symptoms = symptom_list(df,print_top=0)

    cl= [10,25,10,10]
    print_row(['index','symptoms','count','percent of symptoms'],column_lengths=cl)
    for index,i in enumerate(all_symptoms):
        if i[0].upper() in women_repro_symptoms:
            print_row(
                [
                    index,
                    i[0],
                    '{:,.2f}'.format(i[1]),
                    '{:.2f}'.format((i[1]/len(all_symptoms))*100) 
                ],
                column_lengths=cl
                )


    print('\n\n--------------------------------\n\n')

    file_name  = os.path.join(datapath,'women_repro_symptoms_20210808.csv')
    df_symptoms.to_csv(file_name)
    print('saved: ',file_name)


if __name__ == '__main__':

    # main()
    women_issues()

    # print_row(['One','Two','Three','Four'],[20,15,10,5])
    # print_row(['One','Two','Three','Four'],[20,15,10,5])
    # print_row(['One','Two','Three','Four'],[20,15,10,5])
    # print_row(['One','Two','Three','Four'],[20,15,10,5])
