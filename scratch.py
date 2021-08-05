

import pandas as pd

df = pd.DataFrame({'A':[0,1,2,3,4,5,6,7,8,4,1,2,0,1,2,3,4,5,6,6,6,6,6,6]})

# print(df['A'].quantile([0.0,0.25,0.5,0.75,1.0]))


df['A_bucket'] = pd.qcut(
        df['A'],
        q=[0.0, 0.25, 0.5, 0.75, 1.0],
        labels=['0-25','25-50','50-75','75-100'],
        precision=0,
        duplicates='drop',
    )


d0 = df.groupby(by="A_bucket").min()
d1 = df.groupby(by="A_bucket").max()

d2 = pd.merge(d0,d1,how='outer',on='A_bucket')
print(d2)

df0 = df.groupby(by="A_bucket").count()
df0['percent'] = (df0['A']/df0['A'].sum()) * 100

print(df0)

print(df)



# # df0['b_NUMDAYS'] = pd.qcut(
# #                     df0['NUMDAYS'],
# #                     q=[0, .2, .4, .6, .8, 1],
# #                     labels=False,
# #                     precision=0,
# #                     duplicates='drop',
# #                     )

# # labels=['0-10','10-20','20-30','30-40','40-50','50-60','60-70','70-80','80-90','90-100']

# # df0['bin_AGE_YRS'] = pd.cut( df0['AGE_YRS'],
# #     bins=[0,10,20,30,40,50,60,70,80,90,100],
# #     labels=labels
# #     )