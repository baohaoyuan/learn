#!/usr/bin/env python
# coding: utf-8

# #Inventory graphs

# In[20]:


import sys
sys.path.append('/Users/vn060tw/anaconda3/lib/python3.7/site-packages')
import sys, types, os;
has_mfs = sys.version_info > (3, 5);p = os.path.join(sys._getframe(1).$


# In[6]:


import os
import pandas as pd



# In[4]:


os.chdir('/Users/vn060tw/Documents/inventory_modeling')
print( os.getcwd())


# In[17]:


dr=pd.read_csv('inventory_top50_ranking_5yr.csv',quotechar='"')


# In[18]:


dr.head(10)


# In[20]:


df_r=pd.DataFrame(dr,columns=['TRADING_DEPT_ID','TRADING_DEPT_NAME','SKU_ID','TOTAL_ORDERED_QTY','TOTAL_SUBST_QTY','RK'])


# In[31]:


df_r[['TRADING_DEPT_NAME', 'RK']]


# In[45]:


df_rk1=df_r[(df_r['RK']==1)&(df_r['TOTAL_SUBST_QTY']>10000)]


# In[92]:


d=df_rk1.sort_values(by='TOTAL_SUBST_QTY',ascending=0)


# In[95]:


d.head(30)
d.shape


# In[1]:


import numpy as np
import matplotlib.pyplot as plt


# In[96]:


d


# In[97]:


x=d['TRADING_DEPT_NAME']
y=d['TOTAL_SUBST_QTY']


# In[98]:


x.head()
y.head()


# In[99]:



plt.scatter(x,y)

#plt.scatter(df_rk1['TRADING_DEPT_NAME'],df_rk1['TOTAL_SUBST_QTY'])

#plt.scatter(x,y,s=area,c=colors,alpha=0.5)
#colors = (0,0,0)
#c = ("red")
s = np.pi*3

plt.title('Total Substituted Quantify by the top 1 ranked that are larger than 1000 within each Trading Department Name  ')
plt.xlabel('x')
plt.ylabel('y')
plt.xticks(rotation=90)
plt.show()




# In[ ]:





# In[100]:


plt.bar(x, y, align='center', alpha=0.5)
plt.xticks(rotation=90)

