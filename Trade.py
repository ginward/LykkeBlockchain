
# coding: utf-8

# In[1]:

'''
The python script to analyze the trade data for lykke

Author: Jinhua Wang, University of Toronto

January 2017

License: The MIT License (MIT)

Copyright (c) 2016 Jinhua Wang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''


# In[2]:

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
import json
import math
#the transaction log
transaction_log=[]
transaction_log_json=[]


# In[3]:

#pd.read_csv("trade_log_20160801_20161130.csv")


# In[4]:

df=pd.read_csv("trade_log_20160801_20161130.csv")


# In[5]:

df["time"]=pd.to_datetime(df.TradeDt)


# In[6]:

#sort the data by time
df=df.sort_values(by="time")


# In[7]:

#drop the useless columns
df.drop('TraderWalletId',1,inplace=True)
df.drop('MktMakerWalletId',1,inplace=True)
df.drop('UsdVol',1,inplace=True)
df.drop('BtcVol',1,inplace=True)
df.drop('TxHashId',1,inplace=True)
df.drop('BlockchainDt',1,inplace=True)
df.drop('ConfTimeHours',1,inplace=True)
df.drop('ConfTimeMinutes',1,inplace=True)
df.drop('TxUrl',1,inplace=True)
df.drop('MarketOrderId',1,inplace=True)
df.drop('MarketOrderDt',1,inplace=True)
df.drop('LimitOrderld',1,inplace=True)
df.drop('LimitOrderDt',1,inplace=True)
df.drop('TradeDt',1,inplace=True)
df=df.rename(columns={'Asset1':'Sell', 'Asset2':'Buy', 'Qty1':'Sell_Vol', 'Qty2':'Buy_Vol'})
df["Sell_Vol"]=df["Sell_Vol"].abs()
df['Price'] = df['Price'].astype('float64') 
#df


# In[8]:

#the set used to count the number of unique assets trading in the exchange
s={}
#construct the orders and put the orders into the order book
for index, row in df.iterrows():
    if row["Buy"] not in s:
        s[row["Buy"]]=True
    if row["Sell"] not in s:
        s[row["Sell"]]=True
print "the number of unique assets are " + str(len(s))
print "the list of unique assets follows:"
print "**********************************"
for key in s: 
    print "Asset " + key
print "**********************************"


# In[9]:

#we assume all orders transact at the mid price
df_orderbook=df.values.tolist()
log_result = []
i=0
for row in df_orderbook:
    if i<len(df_orderbook)-1:
        i+=1
        current_time=row[7]
        current_date = current_time.date()
        #query downwards to find trades on the same date
        for n_row in df_orderbook[i:]:
            #a trade match found on the same trading day
            if n_row[3]==row[1] and n_row[1]==row[3] and n_row[7].date()==current_date and row[2]!=0 and row[4]!=0 and n_row[2]!=0 and n_row[4]!=0:
                #calculate the average price for the trade
                #for example, sell row[1]=BTC, buy row[3]=USD
                if (row[1]<row[3]):
                    price_1 = float(row[5])
                else:
                    price_1 = float(row[6])
                
                if (n_row[1]<n_row[3]):
                    price_2 = float(n_row[5])
                else:
                    price_2 = float(n_row[6])
                bid=0
                ask=0
                if price_1>price_2:
                    bid=price_2
                    ask=price_1
                else:
                    bid=price_1
                    ask=price_2
                price_avg=(price_1+price_2)/2
                #the quantity that the first trader can buy
                #for example, sell row[1]=BTC, buy row[3]=USD
                if (row[1]<row[3]):
                    qty_buy_1=row[2]
                else:
                    qty_buy_1=row[4]
                #the actual transacted volume
                vol_tmp=0
                #check if the next trader has enough currency to sell 
                #for example, sell n_row[1]=USD, buy n_row[3]=BTC
                if (n_row[1]>n_row[3]):
                    if qty_buy_1<n_row[4]:
                        n_row[4]=n_row[4]-qty_buy_1
                        vol_tmp=qty_buy_1
                        row[2]=0
                    else:
                        vol_tmp=n_row[4]
                        row[2]=row[2]-n_row[4]
                        n_row[4]=0
                else:
                    if qty_buy_1<n_row[2]:
                        n_row[2]=n_row[2]-qty_buy_1
                        vol_tmp=qty_buy_1
                        row[4]=0
                    else:
                        vol_tmp=n_row[2]
                        row[4]=row[4]-n_row[2]
                        n_row[2]=0
                trader1_id=row[0]
                trader2_id=n_row[0]
                #write log of the transaction
                tmp_arr = []
                tmp_arr.append(trader1_id)
                tmp_arr.append(trader2_id)
                tmp_arr.append(bid)
                tmp_arr.append(ask)
                tmp_arr.append(price_avg)
                tmp_arr.append(row[1])
                tmp_arr.append(row[3])
                tmp_arr.append(n_row[1])
                tmp_arr.append(n_row[3])
                tmp_arr.append(vol_tmp)
                tmp_arr.append(row[7].time())
                tmp_arr.append(n_row[7])
                tmp_arr.append(n_row[7].date())
                log_result.append(tmp_arr)
                tmp_log={"trader1":trader1_id, "trader2":trader2_id, "bid":bid, "ask":ask , "price":price_avg, "trader1_sell":row[1], "trader1_buy":row[3], "trader2_sell":n_row[1], "trader2_buy":n_row[3], "vol_trader1_buy/sell":vol_tmp,"start_time":row[7],"transaction_time":n_row[7], "date":n_row[7].date()} 
                transaction_log.append(tmp_log)
                tmp_log_json={"trader1":trader1_id, "trader2":trader2_id, "bid":bid, "ask":ask , "price":price_avg, "trader1_sell":row[1], "trader1_buy":row[3], "trader2_sell":n_row[1], "trader2_buy":n_row[3], "vol_trader1_buy/sell":vol_tmp,"start_time":str(row[7]),"transaction_time":str(n_row[7]), "date":str(n_row[7].date())} 
                transaction_log_json.append(tmp_log_json)
                if(row[2]<=0): 
                    df_orderbook.remove(row)
                    #if row[7].month == 11:
                        #print "remove"+row[0]+" time:" + str(row[7])
                    break
                if(n_row[2]<=0):
                    #remove the transaction since the order has already been filled
                    df_orderbook.remove(n_row)
                    #if n_row[7].month == 11:
                        #print "remove"+n_row[0]+" time:" + str(n_row[7])
log_list = ["trader1", "trader2", "bid", "ask", "price", "trader1_sell", "trader1_buy", "trader2_sell", "trader2_buy", "vol_trader1_buy","start_time","transaction_time", "date"]
transaction_log_df = pd.DataFrame(log_result,columns=log_list)
transaction_log_df.to_csv("python_csv/hypo_trade_log.csv")
df


# In[10]:

def convert_milli_hr(x):
    x = float(x)
    x = x / 1000
    seconds = x % 60
    x /= 60
    minutes = x % 60
    x /= 60
    hours = x % 24
    x /= 24
    days = x
    return hours
#calculate the average transaction time
time_arr = []
for log in transaction_log:
    t1_ms = (log["start_time"].hour*60*60 + log["start_time"].minute*60 + log["start_time"].second)*1000 + log["start_time"].microsecond
    t2_ms = (log["transaction_time"].hour*60*60 + log["transaction_time"].minute*60 + log["transaction_time"].second)*1000 + log["transaction_time"].microsecond
    diff=t2_ms-t1_ms
    if diff>0:
        time_arr.append(diff)
np_time_arr = np.array(time_arr)
    
print "the average trade wait time for all transactions (without market maker) in milliseconds: "+ str(np.mean(np_time_arr))
print "the median is :" + str(np.median(np_time_arr))

avg_time_dict={}
for log in transaction_log:
    tu = ()
    if log["trader1_buy"]> log["trader1_sell"]:
        tu = (log["trader1_buy"], log["trader1_sell"])  
    else:
        tu =(log["trader1_sell"], log["trader1_buy"])
    if log["transaction_time"].month not in avg_time_dict or avg_time_dict[log["transaction_time"].month] is None:
        avg_time_dict[log["transaction_time"].month] = {}
    if tu not in avg_time_dict[log["transaction_time"].month] or avg_time_dict[log["transaction_time"].month][tu] is None:
        avg_time_dict[log["transaction_time"].month][tu] = []
    t1_ms = (log["start_time"].hour*60*60 + log["start_time"].minute*60 + log["start_time"].second)*1000 + log["start_time"].microsecond/1000
    t2_ms = (log["transaction_time"].hour*60*60 + log["transaction_time"].minute*60 + log["transaction_time"].second)*1000 + log["transaction_time"].microsecond/1000
    #if t2_ms-t1_ms>0:
    avg_time_dict[log["transaction_time"].month][tu].append(convert_milli_hr(t2_ms-t1_ms)) 
    
result=[]
for month in avg_time_dict:
    for key in avg_time_dict[month]:
        if len(avg_time_dict[month][key])>0:
            arr = np.array(avg_time_dict[month][key])
            #if month == 11:
                #print key
                #print len(avg_time_dict[month][key])
            key_new = ()
            tmp = []
            if key[0]<key[1]:
                key_new=(key[1],key[0])
            else:
                key_new = key
            tmp.append(key_new)
            tmp.append(month)
            tmp.append(round(np.mean(arr),2))
            tmp.append(round(np.std(arr), 2))
            tmp.append(round(np.median(arr),2))
            tmp.append(len(avg_time_dict[month][key]))
            result.append(tmp)
            #print "the average trade time for "+str(key)+" (without the market maker) in milliseconds is " + str(avg) + " standard deviation is: "+str(std)+" the number of observation is: " + str(len(avg_time_dict[key]))
        else: 
            #print "None enough data for " + str(key)
            pass
header_list = ["Currency Pair", "Month", "Trader Wait Time","Std. Deviation", "Median","Observations"]
df_result = pd.DataFrame(result, columns=header_list)
df_result = df_result.sort(['Currency Pair', 'Month'], ascending=False)
df_result = df_result.reset_index(drop=True)
print df_result.to_latex()
df_result.to_csv("python_csv/hypo_trade_interaction_tim.csv")


# In[11]:

#transaction_log


# In[12]:

print "Total Number of Trades (without the market maker): " + str(len(transaction_log))
header_count_list = ["Currency Pair", "Total number of trades","Percentile"]
result_count=[]
#get the number of trades per currency pair
count_dict = {}
for log in transaction_log:
    tu = ()
    if log["trader1_buy"]<log["trader1_sell"]:
        tu =(log["trader1_sell"],log["trader1_buy"])
    else:
        tu =(log["trader1_buy"],log["trader1_sell"])   
    if tu not in count_dict or count_dict[tu] is None:
        count_dict[tu]=0
    count_dict[tu]+=1
for key in count_dict:
    tmp = []
    tmp.append(key)
    tmp.append(count_dict[key])
    tmp.append(round(float(count_dict[key])/len(transaction_log),4)) 
    result_count.append(tmp)
    #print "Total number of trades (without the market maker) for "+str(key)+" is:" + str(count_dict[key])
df_count = pd.DataFrame(result_count, columns=header_count_list)
df_count = df_count.sort('Currency Pair', ascending=False)
df_count = df_count.reset_index(drop=True)
print df_count.to_latex()
df_count.to_csv("python_csv/trade_summary.csv")


# In[13]:

#calculate the average bid ask spread (Without the Market Maker)
avg_spread={}
avg_spread_mid={}
for log in transaction_log:
    tmp=()
    if log['trader1_buy']<log['trader1_sell']:
        tmp=(log['trader1_buy'], log['trader1_sell'])
    else:
        tmp=(log['trader1_sell'], log['trader1_buy'])
    spread=abs(log['ask']-log['bid'])
    if tmp not in avg_spread or avg_spread[tmp] is None:
        avg_spread[tmp] = []
    if tmp not in avg_spread_mid or avg_spread_mid[tmp] is None:
        avg_spread_mid[tmp] = []
    avg_spread[tmp].append(spread)
    if log["price"]!=0:
        avg_spread_mid[tmp].append(spread/log["price"])
header_list_spread = ["Currency Pair", "Bid-Ask Spread", "Standard Deviation", "Observations"]
result_spread = []
for key in avg_spread: 
    key_new = ()
    tmp = []
    arr = np.array(avg_spread[key])
    ave = np.mean(arr)
    std = np.std(arr)
    if key[0]<key[1]:
        key_new=(key[1],key[0])
    else:
        key_new = key
    tmp.append(key_new)
    tmp.append(round(ave,6))
    tmp.append(round(std,6))
    tmp.append(len(avg_spread[key]))
    result_spread.append(tmp)
df_result_spread = pd.DataFrame(result_spread, columns=header_list_spread)
df_result_spread = df_result_spread.sort('Currency Pair', ascending=False)
df_result_spread = df_result_spread.reset_index(drop=True)
    
header_list_spread_mid = ["Currency Pair", "Bid-Ask Spread/Mid Price", "Standard Deviation", "Observations"] 
result_spread_mid=[]
for key in avg_spread_mid:
    key_new = ()
    tmp = []
    arr_ = np.array(avg_spread_mid[key])
    ave_ = np.mean(arr_) * 10000
    std_ = np.std(arr_)
    if key[0]<key[1]:
        key_new=(key[1],key[0])
    else:
        key_new = key
    tmp.append(key_new)
    tmp.append(round(ave_,6))
    tmp.append(round(std_,6))
    tmp.append(len(avg_spread_mid[key]))
    result_spread_mid.append(tmp)
df_result_spread_mid = pd.DataFrame(result_spread_mid, columns=header_list_spread_mid)
df_result_spread_mid = df_result_spread_mid.sort('Currency Pair', ascending=False)
df_result_spread_mid = df_result_spread_mid.reset_index(drop=True)


# In[14]:

print "*******WITHOUT MARKET MAKER MEASURE*******"
print df_result_spread.to_latex()
df_result_spread.to_csv("without_MM_ave_spread.csv")


# In[15]:

print "*******WITHOUT MARKET MAKER MEASURE*******"
print df_result_spread_mid.to_latex()
df_result_spread_mid.to_csv("without_MM_ave_spread_basis_pts.csv")


# In[16]:

#output the transaction log to file
with open("transaction_log.json", 'wb') as outfile:
    json.dump(transaction_log_json, outfile)


# In[17]:

'''The Roll Measure to infer Bid Ask spread'''
#BTCE-USDBTC.csv
#the dataframe for the bitcoin data
df_btcusd = pd.read_csv("BTCE-USDBTC.csv")
#convert the Date to date object
df_btcusd["Date"]=pd.to_datetime(df_btcusd.Date)
#reinitialize the df_orderbook - the data has been changed since last time
df_orderbook=df.values.tolist()
def filter_direction(df_orderbook, freq):
    '''
    The function to set the trades to a uniform direction
    '''
    #the dictionary for tuple currency pairs and price arrays
    price_dict = {}
    for row in df_orderbook:
        #sell USD, buy BTC
        if row[1]=='USD' and row[3]=='BTC':
            tuple_tmp = ('BTC', 'USD')
            if tuple_tmp not in price_dict or price_dict[tuple_tmp] is None:
                price_dict[tuple_tmp] = {}
            try:
                if freq == "daily":
                    if row[7].date() not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].date()] is None:
                        price_dict[tuple_tmp][row[7].date()] = []
                    price_dict[tuple_tmp][row[7].date()].append(float(row[6])) #InvPrice, Time
                elif freq == "monthly":
                    if row[7].month not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].month] is None:
                        price_dict[tuple_tmp][row[7].month] = []
                    price_dict[tuple_tmp][row[7].month].append(float(row[6]))
                elif freq == "weekly":
                    week = ""
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in price_dict[tuple_tmp] or price_dict[tuple_tmp][week] is None:
                        price_dict[tuple_tmp][week] = []
                    price_dict[tuple_tmp][week].append(float(row[6])) #InvPrice   
                elif freq == "all":
                    if 'all' not in price_dict[tuple_tmp] or price_dict[tuple_tmp]['all'] is None:
                        price_dict[tuple_tmp]['all'] = []
                    price_dict[tuple_tmp]['all'].append(float(row[6]))
            except ValueError:
                pass
        elif row[1]=='BTC' and row[3]=='USD':
            tuple_tmp = ('BTC', 'USD')
            if tuple_tmp not in price_dict or price_dict[tuple_tmp] is None:
                price_dict[tuple_tmp] = {}
            try:
                if freq == "daily":
                    if row[7].date() not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].date()] is None:
                        price_dict[tuple_tmp][row[7].date()] = []
                    price_dict[tuple_tmp][row[7].date()].append(float(row[5])) #Price, Time
                elif freq == "monthly":
                    if row[7].month not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].month] is None:
                        price_dict[tuple_tmp][row[7].month] = []
                    price_dict[tuple_tmp][row[7].month].append(float(row[5]))
                elif freq == "weekly":
                    week = 0
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in price_dict[tuple_tmp] or price_dict[tuple_tmp][week] is None:
                        price_dict[tuple_tmp][week] = []
                    price_dict[tuple_tmp][week].append(float(row[5])) #Price   
                elif freq == "all":
                    if 'all' not in price_dict[tuple_tmp] or price_dict[tuple_tmp]['all'] is None:
                        price_dict[tuple_tmp]['all'] = []
                    price_dict[tuple_tmp]['all'].append(float(row[5]))
            except ValueError:
                pass
        else: 
            if row[1]<row[3]:
                tuple_tmp = (row[1],  row[3])
                if tuple_tmp not in price_dict or price_dict[tuple_tmp] is None:
                    price_dict[tuple_tmp] = {}
                try:
                    if freq == "daily":
                        if row[7].date() not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].date()] is None:
                            price_dict[tuple_tmp][row[7].date()] = []
                        price_dict[tuple_tmp][row[7].date()].append(float(row[6])) #InvPrice
                    elif freq == "monthly":
                        if row[7].month not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].month] is None:
                            price_dict[tuple_tmp][row[7].month] = []
                        price_dict[tuple_tmp][row[7].month].append(float(row[6]))
                    elif freq == "weekly":
                        week = 0
                        if row[7].day < 7:
                            week = str(row[7].month)+"_"+str(1)
                        elif row[7].day >=7 and row[7].day < 14:
                            week = str(row[7].month)+"_"+str(2)
                        elif row[7].day >=14 and row[7].day < 21:
                            week = str(row[7].month)+"_"+str(3)
                        elif row[7].day >=21 and row[7].day <= 31:
                            week = str(row[7].month)+"_"+str(4)      
                        if week not in price_dict[tuple_tmp] or price_dict[tuple_tmp][week] is None:
                            price_dict[tuple_tmp][week] = []
                        price_dict[tuple_tmp][week].append(float(row[6])) #InvPrice       
                    elif freq == "all":
                        if 'all' not in price_dict[tuple_tmp] or price_dict[tuple_tmp]['all'] is None:
                            price_dict[tuple_tmp]['all'] = []
                        price_dict[tuple_tmp]['all'].append(float(row[6]))
                except ValueError:
                    pass
            else:
                tuple_tmp = (row[3],  row[1])
                if tuple_tmp not in price_dict or price_dict[tuple_tmp] is None:
                    price_dict[tuple_tmp] = {}
                try:
                    if freq == "daily":
                        if row[7].date() not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].date()] is None:
                            price_dict[tuple_tmp][row[7].date()] = []
                        price_dict[tuple_tmp][row[7].date()].append(float(row[5])) #Price
                    elif freq == "monthly":
                        if row[7].month not in price_dict[tuple_tmp] or price_dict[tuple_tmp][row[7].month] is None:
                            price_dict[tuple_tmp][row[7].month] = []
                        price_dict[tuple_tmp][row[7].month].append(float(row[5]))
                    elif freq == "weekly":
                        week = 0
                        if row[7].day < 7:
                            week = str(row[7].month)+"_"+str(1)
                        elif row[7].day >=7 and row[7].day < 14:
                            week = str(row[7].month)+"_"+str(2)
                        elif row[7].day >=14 and row[7].day < 21:
                            week = str(row[7].month)+"_"+str(3)
                        elif row[7].day >=21 and row[7].day <= 31:
                            week = str(row[7].month)+"_"+str(4) 
                        if week not in price_dict[tuple_tmp] or price_dict[tuple_tmp][week] is None:
                            price_dict[tuple_tmp][week] = []
                        price_dict[tuple_tmp][week].append(float(row[5])) #Price
                    elif freq == "all":
                        if 'all' not in price_dict[tuple_tmp] or price_dict[tuple_tmp]['all'] is None:
                            price_dict[tuple_tmp]['all'] = []
                        price_dict[tuple_tmp]['all'].append(float(row[5]))
                except ValueError:
                    pass
    return price_dict

def avg_bench_mark(df_orderbook, freq):
    '''
    The function to calculate the benchmark as average price
    '''
    bench_price_dict = {}
    for row in df_orderbook:
        if row[1]=='USD' and row[3]=='BTC':
            tuple_tmp = ('BTC', 'USD')
            if tuple_tmp not in bench_price_dict or bench_price_dict[tuple_tmp] is None:
                bench_price_dict[tuple_tmp] = {}
            try: 
                if freq == "daily":
                    if row[7].date() not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].date()] is None:
                        bench_price_dict[tuple_tmp][row[7].date()] = []
                    bench_price_dict[tuple_tmp][row[7].date()].append(float(row[6])) #InvPrice                    
                elif freq == "monthly":
                    if row[7].month not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].month] is None:
                        bench_price_dict[tuple_tmp][row[7].month] = []
                    bench_price_dict[tuple_tmp][row[7].month].append(float(row[6]))                    
                elif freq == "weekly":
                    week = 0
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][week] is None:
                        bench_price_dict[tuple_tmp][week] = []
                    bench_price_dict[tuple_tmp][week].append(float(row[6])) #InvPrice                    
                elif freq == "all":
                    if 'all' not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp]['all'] is None:
                        bench_price_dict[tuple_tmp]['all'] = []
                    bench_price_dict[tuple_tmp]['all'].append(float(row[6]))                       
            except ValueError:
                pass
        elif row[1]=='BTC' and row[3]=='USD':
            tuple_tmp = ('BTC', 'USD')
            if tuple_tmp not in bench_price_dict or bench_price_dict[tuple_tmp] is None:
                bench_price_dict[tuple_tmp] = {}
            try: 
                if freq == "daily":
                    if row[7].date() not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].date()] is None:
                        bench_price_dict[tuple_tmp][row[7].date()] = []
                    bench_price_dict[tuple_tmp][row[7].date()].append(float(row[5])) #Price                    
                elif freq == "monthly":
                    if row[7].month not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].month] is None:
                        bench_price_dict[tuple_tmp][row[7].month] = []
                    bench_price_dict[tuple_tmp][row[7].month].append(float(row[5]))                    
                elif freq == "weekly":
                    week = 0
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][week] is None:
                        bench_price_dict[tuple_tmp][week] = []
                    bench_price_dict[tuple_tmp][week].append(float(row[5])) #Price                    
                elif freq == "all":
                    if 'all' not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp]['all'] is None:
                        bench_price_dict[tuple_tmp]['all'] = []
                    bench_price_dict[tuple_tmp]['all'].append(float(row[5]))                       
            except ValueError:
                pass            
        if row[1]<row[3]:
            tuple_tmp = (row[1],  row[3])
            if tuple_tmp not in bench_price_dict or bench_price_dict[tuple_tmp] is None:
                bench_price_dict[tuple_tmp] = {}
            try: 
                if freq == "daily":
                    if row[7].date() not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].date()] is None:
                        bench_price_dict[tuple_tmp][row[7].date()] = []
                    bench_price_dict[tuple_tmp][row[7].date()].append(float(row[6])) #InvPrice                    
                elif freq == "monthly":
                    if row[7].month not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].month] is None:
                        bench_price_dict[tuple_tmp][row[7].month] = []
                    bench_price_dict[tuple_tmp][row[7].month].append(float(row[6]))                    
                elif freq == "weekly":
                    week = 0
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][week] is None:
                        bench_price_dict[tuple_tmp][week] = []
                    bench_price_dict[tuple_tmp][week].append(float(row[6])) #InvPrice                    
                elif freq == "all":
                    if 'all' not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp]['all'] is None:
                        bench_price_dict[tuple_tmp]['all'] = []
                    bench_price_dict[tuple_tmp]['all'].append(float(row[6]))                       
            except ValueError:
                pass
        else:
            tuple_tmp = (row[3],  row[1])
            if tuple_tmp not in bench_price_dict or bench_price_dict[tuple_tmp] is None:
                bench_price_dict[tuple_tmp] = {}
            try: 
                if freq == "daily":
                    if row[7].date() not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].date()] is None:
                        bench_price_dict[tuple_tmp][row[7].date()] = []
                    bench_price_dict[tuple_tmp][row[7].date()].append(float(row[5])) #Price                    
                elif freq == "monthly":
                    if row[7].month not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][row[7].month] is None:
                        bench_price_dict[tuple_tmp][row[7].month] = []
                    bench_price_dict[tuple_tmp][row[7].month].append(float(row[5]))                    
                elif freq == "weekly":
                    week = 0
                    if row[7].day < 7:
                        week = str(row[7].month)+"_"+str(1)
                    elif row[7].day >=7 and row[7].day < 14:
                        week = str(row[7].month)+"_"+str(2)
                    elif row[7].day >=14 and row[7].day < 21:
                        week = str(row[7].month)+"_"+str(3)
                    elif row[7].day >=21 and row[7].day <= 31:
                        week = str(row[7].month)+"_"+str(4) 
                    if week not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp][week] is None:
                        bench_price_dict[tuple_tmp][week] = []
                    bench_price_dict[tuple_tmp][week].append(float(row[5])) #Price                    
                elif freq == "all":
                    if 'all' not in bench_price_dict[tuple_tmp] or bench_price_dict[tuple_tmp]['all'] is None:
                        bench_price_dict[tuple_tmp]['all'] = []
                    bench_price_dict[tuple_tmp]['all'].append(float(row[5]))                       
            except ValueError:
                pass
    result_dict = {}
    for key in bench_price_dict:
        if key not in result_dict or result_dict[key] is None: 
            result_dict[key]={}
        for k in bench_price_dict[key]:
            if k not in result_dict[key] or result_dict[key][k] is None:
                result_dict[key][k] = 0
            arr = np.array(bench_price_dict[key][k])
            result_dict[key][k] = np.average(arr)
    return result_dict
        
def bench_mark(ref_btc_df, freq):
    '''
    The function to calculate the benchmark for BTC/USD Pair 
    '''
    #convert ref_btc_df to a list 
    ref_btc_df_list_tmp=ref_btc_df.values.tolist()
    ref_btc_df_dict = {}
    #get the set of reference USD/BTC prices
    for row in ref_btc_df_list_tmp:
        ref_btc_df_dict[row[0]]=row[4]
    #provide a benchmark for price_dict
    bench_price_dict = {}   
    if freq == "daily":
        for key in ref_btc_df_dict:
            bench_price_dict[key.date()]=ref_btc_df_dict[key]
    elif freq == "monthly":
        ave = {}
        for key in ref_btc_df_dict:
            if key.month not in ave or ave[key.month] is None:
                ave[key.month] = []
            ave[key.month].append(ref_btc_df_dict[key])
        for key in ave:
            arr = ave[key]
            bench_price_dict[key] = sum(arr)/len(arr)
    elif freq == "weekly":
        ave = {}
        for key in ref_btc_df_dict:
            week = 0
            if key.day < 7:
                week = str(key.month)+"_"+str(1)
            elif key.day >= 7 and key.day < 14:
                week = str(key.month)+"_"+str(2)
            elif key.day >= 14 and key.day < 21:
                week = str(key.month)+"_"+str(3)
            elif key.day >=21 and key.day <= 31:
                week = str(key.month)+"_"+str(4)
            if week not in ave or ave[week] is None:
                ave[week]=[]
            ave[week].append(ref_btc_df_dict[key])
        for key in ave: 
            arr = ave[key]
            bench_price_dict[key] = sum(arr)/len(arr)
    elif freq == "all":
        ave = {}
        ave["all"]=[]
        for key in ref_btc_df_dict:
            ave["all"].append(ref_btc_df_dict[key])
        for key in ave:
            arr = ave[key]
            bench_price_dict[key] = sum(arr)/len(arr)
    return bench_price_dict

def autocovariance(Xi, N, k, Xs, Xs_):
    '''
    To calculate the autocovariance
    '''
    N=float(N)
    autoCov = 0
    for i in np.arange(1, N-k):
        autoCov += ((Xi[i+k])-Xs_)*(Xi[i]-Xs)
    return float((1/(N-1)))*autoCov

def calculate_roll(df_orderbook, ref_btc_df, freq, nov):
    '''
    Function to calculate the roll measure according to the frequency passed in.
    When nov is True, this script calculates the Novemeber data only. Because the Nov. data is speical in the way
    that the data volume is large. 
    '''
    bench_dict = bench_mark(ref_btc_df, freq) #the benchmark for usd/btc pair
    avg_bench_dict = avg_bench_mark(df_orderbook, freq)
    price_dict = filter_direction(df_orderbook, freq)
    header_list_spread_rolls = ["Currency Pair("+freq+")", "Average Spread", "Standard Deviation", "Observations"] 
    result_spread_rolls=[]
    header_list_spread_noon = ["Currency Pair("+freq+")", "Average Spread/Noon Rate (Basis Points)", "Standard Deviation", "Observations"]
    result_spread_rolls_noon = []
    header_list_roll_avg = ["Currency Pair("+freq+")", "Roll/Ave. Price", "Standard Deviation", "Observations"]
    result_roll_avg = []
    for key in price_dict:
        #to calculate the average value
        ave = []
        #to calculate the average spread/noon rate
        ave_noon = []
        #average spread / average rate 
        roll_ave = []
        #print "*****"+str(key)+" pair starts"+"*****"
        pair_dict = price_dict[key]
        #calcualte the roll measure based on the daily data
        for pair_key in pair_dict:
            #Prof. Park's speical request to calcualte Nov results only 
            if freq == "daily" and nov == True:
                if pair_key.month !=11:
                    continue
            #print "***"+str(pair_key)+" starts ***"
            if len(pair_dict[pair_key])>4:
                price_arr = pair_dict[pair_key]
                #calculate the price delta 
                price_delta_tmp = []
                j=0
                for price in price_arr:
                    if j+1<len(price_arr):
                        tmp_delta = price_arr[j+1]-price_arr[j]
                        price_delta_tmp.append(tmp_delta)
                        j+=1
                Xi = np.array(price_delta_tmp[1:])
                Xi_ = np.array(price_delta_tmp[2:])
                N = np.size(Xi)
                k = 1
                Xs = np.average(Xi)
                Xs_ = np.average(Xi_)
                auto_corr = autocovariance(Xi, N, k, Xs, Xs_)   
                #print "the autocorrelation is: "
                #print auto_corr
                if auto_corr<0:
                    spread = 2 * (math.sqrt(-auto_corr))
                    ave.append(spread)
                    if pair_key in bench_dict:
                        if key == ("BTC", "USD"):
                            ave_noon.append((spread/bench_dict[pair_key])*10000)
                        else:
                            ave_noon.append(0)
                    if key in avg_bench_dict and pair_key in avg_bench_dict[key]:
                        roll_ave.append((spread/avg_bench_dict[key][pair_key])*10000)
                    #print "the roll measure bid and ask spread is:"
                    #print str(spread)
                else:
                    #print "Error: "+"autocorrelation should be negative!"
                    pass
            else:
                #print "The data for "+str(pair_key)+" is not enough (less than 3) for calculation"
                pass
            #print "***"+str(pair_key)+" ends ***"
        if len(ave)>0:
            if key[0]<key[1]:
                key_new=(key[1],key[0])
            else:
                key_new = key
            tmp = []
            arr = np.array(ave)
            mean = np.mean(arr)
            std = np.std(arr)
            tmp.append(key_new)
            tmp.append(round(mean,6))
            tmp.append(round(std,6))
            tmp.append(round(len(ave),6))
            result_spread_rolls.append(tmp)
            #print str(key)+" average spread is: "+str(mean)+" the standard deviation is: "+str(std)
            tmp_noon = []
            arr_noon = np.array(ave_noon)
            mean_noon = np.mean(arr_noon)
            std_noon = np.std(arr_noon)
            tmp_noon.append(key_new)
            tmp_noon.append(round(mean_noon,6))
            tmp_noon.append(round(std_noon,6))
            tmp_noon.append(round(len(ave),6))
            result_spread_rolls_noon.append(tmp_noon)
            #roll/ave
            tmp_avg = []
            arr_avg = np.array(roll_ave)
            mean_avg = np.mean(arr_avg)
            std_avg = np.std(arr_avg)
            tmp_avg.append(key_new)
            tmp_avg.append(round(mean_avg,6))
            tmp_avg.append(round(std_avg,6))
            tmp_avg.append(round(len(ave),6))
            result_roll_avg.append(tmp_avg)
            #print str(key)+" average spread/noon rate (in basis points) is:"+str(mean_noon*10000) + " the standard deviation is: "+str(std_noon)
            #print str(key)+" "+str(len(ave))+" days used in the calculation"
        else:
            pass
            #print str(key) + " not enough data "
        #print "*****"+str(key)+" pair ends"+"*****"
    df_result_spread_rolls = pd.DataFrame(result_spread_rolls, columns=header_list_spread_rolls)
    df_result_spread_rolls = df_result_spread_rolls.sort("Currency Pair("+freq+")", ascending=False)
    df_result_spread_rolls = df_result_spread_rolls.reset_index(drop=True)
    
    df_result_spread_rolls_noon = pd.DataFrame(result_spread_rolls_noon, columns=header_list_spread_noon)
    df_result_spread_rolls_noon=df_result_spread_rolls_noon.sort("Currency Pair("+freq+")", ascending=False)
    df_result_spread_rolls_noon=df_result_spread_rolls_noon.reset_index(drop=True)
    
    df_result_spread_rolls_ave = pd.DataFrame(result_roll_avg, columns=header_list_roll_avg)
    df_result_spread_rolls_ave=df_result_spread_rolls_ave.sort("Currency Pair("+freq+")", ascending=False)
    df_result_spread_rolls_ave=df_result_spread_rolls_ave.reset_index(drop=True)
    
    return (df_result_spread_rolls, df_result_spread_rolls_noon, df_result_spread_rolls_ave)
    
def calculate_noon_rate(df_orderbook, ref_btc_df):
    '''
    Function to calculate direction * (price-noon)/noon 
    USD/BTC Pair
    '''
    #convert ref_btc_df to a list 
    ref_btc_df_list_tmp=ref_btc_df.values.tolist()
    ref_btc_df_dict = {}
    #get the set of reference USD/BTC prices
    for row in ref_btc_df_list_tmp:
        ref_btc_df_dict[row[0].date()]=float(row[4])
    #the result 
    ave = []
    for row in df_orderbook:
        date_row = row[7].date()
        #sell USD, buy BTC
        if row[1]=='USD' and row[3]=='BTC' and date_row in ref_btc_df_dict:
            direction = 1
            price = float(row[6])
            relative = direction * (price-ref_btc_df_dict[date_row])/ref_btc_df_dict[date_row]
            ave.append(relative)
        #sell BTC, buy USD
        elif row[1]=='BTC' and row[3]=='USD' and date_row in ref_btc_df_dict:
            direction = -1
            price = float(row[5])
            relative = direction * (price-ref_btc_df_dict[date_row])/ref_btc_df_dict[date_row]
            ave.append(relative)
    arr = np.array(ave)
    mean = np.mean(arr)
    std = np.std(arr)
    print "the (daily) average direction * (price-noon)/noon  for USD-BTC (in basis points) is: "+str(mean * 10000)+" the standard deviation is: "+str(std)
    
daily_roll = calculate_roll(df_orderbook, df_btcusd, "daily", False)
monthly_roll = calculate_roll(df_orderbook, df_btcusd, "monthly", False)
nov_roll = calculate_roll(df_orderbook, df_btcusd, "daily", True)
weekly_roll = calculate_roll(df_orderbook, df_btcusd, "weekly", False)
all_roll = calculate_roll(df_orderbook, df_btcusd, "all", False)
calculate_noon_rate(df_orderbook, df_btcusd)
#df_btcusd


# In[18]:

print "*******ROLLS MEASURE*******"
print daily_roll[0].to_latex()
daily_roll[0].to_csv("python_csv/daily_rolls.csv")


# In[19]:

print "*******ROLLS MEASURE*******"
print daily_roll[1].to_latex()
daily_roll[1].to_csv("python_csv/daily_rolls_basis_pts.csv")


# In[20]:

print "*******ROLLS MEASURE*******"
print daily_roll[2].to_latex()
daily_roll[2].to_csv("python_csv/daily_rolls_divide_avg.csv")


# In[21]:

print nov_roll[0].to_latex()
nov_roll[0].to_csv("python_csv/Nov_Rolls.csv")


# In[22]:

print nov_roll[1].to_latex()
nov_roll[0].to_csv("python_csv/Nov_Rolls_BPS.csv")


# In[23]:

print "*******ROLLS MEASURE*******"
print monthly_roll[0].to_latex()
monthly_roll[0].to_csv("python_csv/monthly_rolls.csv")


# In[24]:

print "*******ROLLS MEASURE*******"
print monthly_roll[1].to_latex()
monthly_roll[1].to_csv("python_csv/monthly_rolls_basis_pts.csv")


# In[25]:

print "*******ROLLS MEASURE*******"
print monthly_roll[2].to_latex()
monthly_roll[2].to_csv("python_csv/monthly_rolls_divide_avg.csv")


# In[26]:

print "*******ROLLS MEASURE*******"
print weekly_roll[0].to_latex()
weekly_roll[0].to_csv("python_csv/weekly_rolls.csv")


# In[27]:

print "*******ROLLS MEASURE*******"
print weekly_roll[1].to_latex()
weekly_roll[1].to_csv("python_csv/weekly_rolls_basis_pts.csv")


# In[28]:

print "*******ROLLS MEASURE*******"
print weekly_roll[2].to_latex()
weekly_roll[2].to_csv("python_csv/weekly_rolls_divide_avg.csv")


# In[29]:

print "*******ROLLS MEASURE*******"
print all_roll[0].to_latex()
all_roll[0].to_csv("python_csv/all_rolls.csv")


# In[30]:

print "*******ROLLS MEASURE*******"
print all_roll[1].to_latex()
all_roll[1].to_csv("python_csv/all_rolls_basis_pts.csv")


# In[ ]:

print "*******ROLLS MEASURE*******"
print all_roll[2].to_latex()
all_roll[2].to_csv("python_csv/all_rolls_divide_avg.csv")


# In[ ]:

#draw the graph of trades
def draw_network(transaction_log):
    G=nx.Graph()
    #dictionary to record the volume that a trader trades
    freq={}
    #define the trade relations between the two traders, {(trader1, trader2), frequency}
    relations = {}
    #the array of sizes for graph drawing
    sizes=[]
    for log in transaction_log:
        if log["trader1"] not in freq or freq[log["trader1"]] is None:
            freq[log["trader1"]]=1
        else: 
            freq[log["trader1"]]+=1
        if log["trader2"] not in freq or freq[log["trader2"]] is None:
            freq[log["trader2"]]=1
        else:
            freq[log["trader2"]]+=1
    for log in transaction_log:
        tmp = ()
        if log["trader1"] < log["trader2"]:
            tmp = (log["trader1"],log["trader2"])
        else:
            tmp = (log["trader2"],log["trader1"])
        #if the edges are already connected 
        if tmp in relations:
            relations[tmp] += 1
        else: 
            relations[tmp] = 1
        G.add_edge(log["trader1"],log["trader2"], weight=relations[tmp]*10)
    elarge=[(u,v) for (u,v,d) in G.edges(data=True) if d['weight'] >0.5]
    esmall=[(u,v) for (u,v,d) in G.edges(data=True) if d['weight'] <=0.5]
    #get the list of nodes from the graph
    nodes=G.nodes()
    for node in nodes:
        if node in freq:
            #the maximum size
            if freq[node]>=500:
                sizes.append(500*10)
            else:
                sizes.append(freq[node]*10)
        else:
            sizes.append(0)
    #pos=nx.spring_layout(G,k=0.5,iterations=500) # positions for all nodes
    pos=nx.random_layout(G)
    # nodes
    nx.draw_networkx_nodes(G,pos,node_size=sizes)
    # edges
    nx.draw_networkx_edges(G,pos,edgelist=elarge,
                        width=1)
    nx.draw_networkx_edges(G,pos,edgelist=esmall,
                        width=1,alpha=0.5,edge_color='b',style='dashed')
    plt.axis('off')
    plt.savefig("transaction_network.png") # save as png
    plt.show() # display
    
#draw_network(transaction_log)

'''
The function that draws the node for certain sizes only
'''
def draw_limited_network(transaction_log):
    G=nx.Graph()
    #dictionary to record the volume that a trader trades
    freq={}
    #define the trade relations between the two traders, {(trader1, trader2), frequency}
    relations = {}
    #the array of sizes for graph drawing
    sizes=[] 
    for log in transaction_log:
        if log["trader1"] not in freq or freq[log["trader1"]] is None:
            freq[log["trader1"]]=1
        else: 
            freq[log["trader1"]]+=1
        if log["trader2"] not in freq or freq[log["trader2"]] is None:
            freq[log["trader2"]]=1
        else:
            freq[log["trader2"]]+=1
    for log in transaction_log:
        if log["trader1"] in freq and freq[log["trader1"]]>5 or log["trader2"] in freq and freq[log["trader2"]]>5:
            if (log["trader1_buy"]=="BTC" and log["trader1_sell"] =="USD") or (log["trader1_buy"]=="USD" and log["trader1_sell"] =="BTC") or (log["trader1_buy"]=="LKK" and log["trader1_sell"] =="USD") or (log["trader1_buy"]=="USD" and log["trader1_sell"] =="LKK") or (log["trader1_buy"]=="BTC" and log["trader1_sell"] =="LKK") or (log["trader1_buy"]=="LKK" and log["trader1_sell"] =="BTC"):
                tmp = ()
                if log["trader1"] < log["trader2"]:
                    tmp = (log["trader1"],log["trader2"])
                else:
                    tmp = (log["trader2"],log["trader1"])
                #if the edges are already connected 
                if tmp in relations:
                    relations[tmp] += 1
                else: 
                    relations[tmp] = 1
                G.add_edge(log["trader1"],log["trader2"], weight=relations[tmp])
    elarge=[(u,v) for (u,v,d) in G.edges(data=True) if d['weight'] >0.5]
    esmall=[(u,v) for (u,v,d) in G.edges(data=True) if d['weight'] <=0.5]
    #get the list of nodes from the graph
    nodes=G.nodes()
    for node in nodes:
        if node in freq:
            #the maximum size
            if freq[node]>=500:
                sizes.append(500*10)
            else:
                sizes.append(freq[node]*10)
        else:
            sizes.append(0)
    pos=nx.spring_layout(G,k=0.028,iterations=300) # positions for all nodes
    #pos=nx.random_layout(G)
    # nodes
    nx.draw_networkx_nodes(G,pos,node_size=sizes)
    # edges
    nx.draw_networkx_edges(G,pos,edgelist=elarge,
                        width=1)
    nx.draw_networkx_edges(G,pos,edgelist=esmall,
                        width=1,alpha=0.34,edge_color='b',style='dashed')
    plt.axis('off')
    plt.savefig("transaction_network.eps" ,format='eps', dpi=2000) # save as eps
    plt.savefig("transaction_network.png",dpi=2000) # save as png
    plt.show() # display
    
draw_limited_network(transaction_log)

