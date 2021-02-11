#!/usr/bin/env python
# coding: utf-8

# In[3]:



import os
import pandas as pd
import numpy as np
import datetime
import requests
import json
from dateutil import tz


# In[159]:


start_dt=(datetime.datetime.utcnow()+ datetime.timedelta(days=1)).strftime('%Y-%m-%d')

end_dt=(datetime.datetime.utcnow()+ datetime.timedelta(days=1)).strftime('%Y-%m-%d')

ts=datetime.datetime.utcnow().strftime("%m%d_%H%M")


# In[4]:


dir='/Users/vn060tw/Documents/crossover'
#dir='/app/crossover_test/'


# In[160]:


col_names=['postcode_sector', 'sub_pc', 'store_from', 'store_to', 'lat','lon','ct']

str_api=pd.read_csv(dir+'/input/str_post_lat_lon.csv',skiprows=1,names=col_names)
str_api.head()
str_api.columns


# In[161]:


str_data=pd.DataFrame(str_api)


# In[22]:


str_test=str_data.head(20)

strList = [] #empty list
for i, r in str_test.iterrows(): 
    mylist = [r.store_from, r.store_to, r.lat, r.lon]
    strList.append(mylist)


# In[162]:


str_test


# In[28]:


strList[1][0]


# In[29]:


strList[0][2]


# In[34]:


#store 1 payload file

store_1=[0]*len(str_test)
for i in range(len(str_test)): 
    with open (dir+'/input/req.json','r') as r_data:
        store_1[i]=json.load(r_data)                  
        store_1[i]['payload']['startDate']=start_dt
        store_1[i]['payload']['endDate']=end_dt
        store_1[i]['payload']['deliveryStartPointExternalId']=strList[i][0]
        store_1[i]['payload']['customerInfo']['customerAddress']['latitude']=strList[i][2]
        store_1[i]['payload']['customerInfo']['customerAddress']['longitude']=strList[i][3]
        fn=str(strList[i][0])+'_'+str(ts)+'_'+str(strList[i][2])+'_'+str(strList[i][3])
    #with open(dir+'/input/request_file'+fn+'_s1.json', 'w') as p:
       #json.dump(store_1, p) 


# In[37]:


store_1[10]


# In[42]:


#store 1 API calls
import requests
api_url = 'http://asda.rovr-api.prod.walmart.com/rovr-app/services/customer/slot/view'
headers = {'content-type': 'application/json' }

re1 = []
for i in range(len(str_test)):
    r1 = requests.post(api_url, json=store_1[i], headers = headers)
    #re.append({'store_id':store_1['payload']['deliveryStartPointExternalId']})
    re1.append(r1.json())
    a='_'+str(strList[i][0])+'_'+str(ts)+'_'+str(strList[i][2])+'_'+str(strList[i][3])
    with open(dir+'/output/res'+a+'_s1.json', 'w') as df:
        json.dump(re1, df)
    del re1[:] 


# In[ ]:


#####store 2 payload

store_2=[0]*len(str_test)
for i in range(len(str_test)): 
    with open (dir+'/input/req.json','r') as r_data:
        store_2[i]=json.load(r_data)                  
        store_2[i]['payload']['startDate']=start_dt
        store_2[i]['payload']['endDate']=end_dt
        store_2[i]['payload']['deliveryStartPointExternalId']=strList[i][1]
        store_2[i]['payload']['customerInfo']['customerAddress']['latitude']=strList[i][2]
        store_2[i]['payload']['customerInfo']['customerAddress']['longitude']=strList[i][3]
        fn=str(strList[i][1])+'_'+str(ts)+'_'+str(strList[i][2])+'_'+str(strList[i][3])
    #with open(dir+'/input/request_file'+fn+'_s2.json', 'w') as p:
       #json.dump(store_2, p)        

#store 2 API calls

import requests
api_url = 'http://asda.rovr-api.prod.walmart.com/rovr-app/services/customer/slot/view'
headers = {'content-type': 'application/json' }

re2 = []
for i in range(len(str_test)):
    r2 = requests.post(api_url, json=store_2[i], headers = headers)
    #re.append({'store_id':store_2['payload']['deliveryStartPointExternalId']})
    re2.append(r2.json())
    a='_'+str(strList[i][1])+'_'+str(ts)+'_'+str(strList[i][2])+'_'+str(strList[i][3])
    with open(dir+'/output/res'+a+'_s2.json', 'w') as df:
        json.dump(re2, df)
    del re2[:] 


# In[ ]:


# scp app@10.117.131.150://app/crossover_test/download/*.json .


# In[7]:


dr='/Users/vn060tw/Documents/crossover/output/' 
dia = []
for x in os.listdir(dr):
    if x.endswith('.json'):
        fs=x.split('_')
        fs=x.split('_')
        f=fs[1]+','+fs[2]+','+fs[3]+','+fs[4]+','+fs[5]+','+fs[len(fs)-1].split('.')[0]
        with open (dr+x,'r') as re_data:
            dat=json.load(re_data) 
            for i in range(len(dat)):
                for j in range(len(dat[i]['payload']['slotDays'])):
                    for k in range(len(dat[i]['payload']['slotDays'][j]['slots'])):
            #printing data for the values in that interval(startDataTime, endDataTime, status)
            #print(da[i]['payload']['slotDays'][j]['slots'][k]['startDateTime'],",",da[i]['payload']['slotDays'][j]['slots'][k]['endDateTime'],",",da[i]['payload']['slotDays'][j]['slots'][k]['status'])
                        dia.append(dat[i]['payload']['slotDays'][j]['slots'][k]['startDateTime']+","
                                   +dat[i]['payload']['slotDays'][j]['slots'][k]['endDateTime']+","
                                   +dat[i]['payload']['slotDays'][j]['slots'][k]['status']+","+f)


# In[8]:


dia[35640]


# In[14]:


with open('res_d.csv', 'w') as fi:
    fi.writelines("%s\n" % place for place in dia)
df=pd.read_csv('res_d.csv')
df.columns=['startDateTime','endDateTime','status','store','req_dt','req_ts','lat','lon','store_type'] 


# In[13]:


df.head()
df_s1 = df[df.store_type=='s1'] 
print(df_s1.shape)


# In[ ]:





# In[18]:


df.groupby('req_ts').count()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[221]:


df_s2 = df[df.store_type=='s2'] 
print(df_s2.shape)


# In[222]:


res1=pd.DataFrame(pd.merge(df_s1,df_s2,on=['startDateTime','endDateTime','req_dt','req_ts','lat','lon']))

type(res1)


# In[223]:


res1.shape


# In[224]:


res1.head()


# In[225]:



res1.columns=['startDateTime','endDateTime','status_1','store_from','req_dt','req_ts','lat','lon','store_type_1','status_2','store_to','store_type_2']


# In[226]:


final=pd.DataFrame(pd.merge(res1,str_test,on=['store_from','store_to','lat','lon']))


# In[227]:


final.shape


# In[213]:


os.getcwd()


# In[136]:


final


# In[228]:


final.to_csv('/Users/vn060tw/Documents/crossover/final_re6.csv', encoding='utf-8', index=False)


# In[ ]:


re7=final[final[req_ts]]


# In[1]:


re8=final.groupby['req_ts']


# In[ ]:


f2 = df.groupby(["SCIENTIFIC NAME" , "OBSERVATION COUNT"])["SCIENTIFIC NAME"].count()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[140]:


result=final[(final['status_1']=="UNAVAILABLE") & (final['status_2']=="AVAILABLE")]


# In[141]:


result.shape


# In[150]:


s1u_s2a=result.groupby('sub_pc').size().reset_index(name='counts')


# In[147]:


s1u=final[final['status_1']=="UNAVAILABLE"].groupby('sub_pc').size().reset_index(name='counts')


# In[148]:


s1u


# In[151]:


s1u_s2a 


# In[152]:


s1us2=pd.DataFrame(pd.merge(s1u_s2a,s1u,on=['sub_pc']))


# In[153]:


s1u_s2a_cnt=result.groupby('sub_pc').size().reset_index(name='counts')


# In[154]:


s1u_s2a_cnt


# In[146]:


ua.dtypes


# In[118]:


os.getcwd()


# In[117]:


final.to_csv('final.csv', encoding='utf-8', index=False)

