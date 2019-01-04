import sqlite3
import pandas as pd
import numpy
import sqlite3
timeframe=['2015-01']
connection=sqlite3.connect("{}.db".format(timeframe))
c=connection.cursor()
limit=5000
last_unix=0
cur_lenght=limit
counter=0
test_done=False
while cur_lenght==limit:
    df=pd.read_sql("SELECT * FROM parent_reply WHERE UNIX > {} AND PARENT NOT NULL AND SCORE >0 ORDER BY UNIX ASC LIMIT {}".format(last_unix,limit),connection)
    last_unix=df.tail(1)['UNIX'].values[0]
    cur_lenght=len(df)
    if not test_done:
        with open("test.from",'a',encoding='utf8') as f:
            for content in df['parent'].values:
                f.write(content+'\n')
                with open("test.from", 'a', encoding='utf8') as f:
                    for content in df['comment'].values:
                        f.write(content + '\n')
    counter+=1
    if counter %20==0:
        print(counter*limit,"rows completed so far")