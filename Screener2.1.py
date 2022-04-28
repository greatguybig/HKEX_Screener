from tkinter import *
import sqlite3
import pandas as pd
import numpy as np
import pandas_datareader.data as web
import datetime as dt
import threading
import warnings
warnings.filterwarnings('ignore')
import time
import sys

root = Tk()
root.title('HK Exchange Screener 2.1')
width = root.winfo_screenwidth()
height = root.winfo_screenheight()
root.geometry('%dx%d+0+0' % (width,height))


def updatedatabase():
    conn = sqlite3.connect('Screener.db')
    url = 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx'
    start = dt.date(2020,1,1)
    end = dt.date.today()
    symbol = pd.read_excel(url,header = 1,usecols = [0,1,2],skiprows = [0],dtype = str)
    symbol = symbol[symbol.Category =='Equity']
    tklist = list(symbol['Stock Code'])
    for tk in tklist:
        try:
            tker = tk[1:5]+'.HK'
            price = web.DataReader(tker,'yahoo',start,end)
            rows = price.shape[0]
            lastday = price.index[-1].date()
            delta = dt.timedelta(days = 5)
            if rows < 20 or (dt.date.today()-lastday > delta) :
                symbol = symbol[symbol['Stock Code'] != tk]
                continue
        except:
            symbol = symbol[symbol['Stock Code'] != tk]
            continue
        root.update_idletasks()
        stocklbl = Label(root,text = 'Importing '+tker)
        stocklbl.grid(row = 0,column = 1)
        root.update_idletasks()
        price.to_sql(name = tker,con = conn,if_exists = 'replace')
    stocklbl.grid_forget()
    symbol.to_sql(name = 'symbols',con = conn,index = False,if_exists = 'replace')
    finishlbl = Label(root,text = 'Database Updated')
    finishlbl.grid(row = 0,column = 1)
    root.update_idletasks()
    conn.close
    sys.exit()
    return
def updatestock(code):
    conn = sqlite3.connect('Screener.db')
    olddata = pd.read_sql_query(f"select * from '{code}'",conn,index_col = 'Date',parse_dates = ['Date'])
    oneday = dt.timedelta(days = 1)
    start = olddata.index[-1].date()+oneday
    end = dt.date.today()
    try:
        newdata = web.DataReader(code,'yahoo',start,end)
        newdata.to_sql(name = code,con = conn,if_exists = 'append')
    except:
        pass
    dedup = f"delete from '{code}' where rowid not in (select min(rowid) from '{code}' group by Date)"
    conn.execute(dedup)
    conn.commit()
    conn.close()
    return
def updatedata():
    conn = sqlite3.connect('Screener.db')
    oldlist = pd.read_sql_query('select * from symbols',conn)
    for tk in oldlist['Stock Code']:
        ticker = tk[1:5]+'.HK'
        updatestock(ticker)
        root.update_idletasks()
        stocklbl2 = Label(root,text = 'Updating '+ticker)
        stocklbl2.grid(row = 0,column = 4)
    stocklbl2.grid_forget()
    now = dt.datetime.now()
    stocklbl2.grid_forget()
    finishlbl = Label(root,text = 'Data Updated at '+now.strftime('%Y-%m-%d %H:%M:%S'))
    finishlbl.grid(row = 0,column = 4)
    root.update_idletasks()
    conn.close()
    sys.exit()
    return
updatebt1 = Button(root,text = 'Build Database',command = threading.Thread(target = updatedatabase).start)
updatebt1.grid(row = 0,column = 0)
updatebt2 = Button(root,text = 'Update Existing Data',command = threading.Thread(target = updatedata).start)
updatebt2.grid(row = 0,column = 3)
t1label = Label(root,text = 'Set T1:')
t1label.grid(row = 1,column = 0)
t1in = Entry(root,width = 30)
t1in.grid(row = 1,column = 1)
t2label = Label(root,text = 'Set T2:')
t2label.grid(row = 1,column = 2)
t2in = Entry(root,width = 30)
t2in.grid(row = 1,column = 3)
klabel = Label(root,text = 'Set K:')
klabel.grid(row = 1,column = 4)
kin = Entry(root,width = 30)
kin.grid(row = 1,column = 5)
def receive():
    global t1,t2,k
    t1 = int(t1in.get())
    t2 = int(t2in.get())
    k = float(kin.get())
    accept = Label(root,text = 'Parameters Received')
    accept.grid(row = 1,column = 7)
    return

def screener(histor,t1,t2,k):
    histor['average'] = histor.Close.rolling(window = 50).mean()
    number = list(histor.index)
    compare = True
    for i in number[-t1:]:
        if histor['Low'][i] <= histor['average'][i]:
            compare = False
            break
    fluct = True
    for i in number[-t2:]:
        judge1 = (histor['Low'][i]-histor['average'][i])/histor['average'][i]
        judge2 = (histor['High'][i]-histor['average'][i])/histor['average'][i]
        if abs(judge1) >= k/100 or abs(judge2) >= k/100:
            fluct = False
            break
    return compare*fluct

def loop():
    global t1,t2,k
    for label in root.grid_slaves():
        if int(label.grid_info()['row'])>7 or \
        (int(label.grid_info()['row'])== 1 and int(label.grid_info()['column'])==7) or \
        (int(label.grid_info()['row'])== 6 and int(label.grid_info()['column'])==1):
            label.grid_remove()
    root.update_idletasks()
    conn = sqlite3.connect('Screener.db')
    statuslabel = Label(root,text = 'Status:')
    statuslabel.grid(row = 6,column = 0)
    resultlabel = Label(root,text = 'Result:')
    resultlabel.grid(row = 7,column = 0,sticky = S)
    start = dt.date(2010,1,1)
    end = dt.date.today()
    symbol = pd.read_sql_query('SELECT * FROM symbols',conn)    
    i = 7
    for tk in symbol['Stock Code']:
        tker = tk[1:5]+'.HK'
        tklabel = Label(root,text = 'Evaluating '+tker)
        tklabel.grid(row = 6,column = 1,sticky = S)
        data = pd.read_sql_query(f"select * from '{tker}'",conn,index_col = 'Date',parse_dates = ['Date'])
        data = data.reset_index()
        select = screener(data,t1,t2,k)
        if select:
            i += 1
            label = Label(root,text = tker+' is selected')
            label.grid(row = (i-8)%18+8,column = (i-8)//18)
        root.update_idletasks()
    
    completelabel = Label(root,text = 'Jobs Successfully Done!')
    completelabel.grid(row = 6,column = 1,sticky = S)
    root.update_idletasks()
    conn.close()
    sys.exit()
t1 = 0
t2 = 0
k = 0
def loopfunc():
    t = threading.Thread(target = loop)
    t.start()
    return
confirm = Button(root,text = 'Confirm',command = receive)
confirm.grid(row = 1,column = 6)
screen = Button(root,text = 'Start Screening',command =
                loopfunc)
screen.grid(row = 5,column = 0)
condition = Label(root,text = 'Conditions:')
condition.grid(row = 2,column = 0)
condition1 = Label(root,text = '''1 In the recent T1 days, the stock prices are all above the average in the past T2 days''')
condition1.grid(row = 3,columnspan=5 )
condition2 = Label(root,text = '''2 The fluctuations of the stock price in the past T2 days are within K% of the average''')
condition2.grid(row = 4,columnspan=5)


root.mainloop()
