import psycopg2
import pandas as pd
from datetime import datetime

from lib import learn
import time

start = time.process_time()
# load settings from file
f = open("settings.txt", "r")
norm_names = f.readline().strip().split('=')[1].split(',')
cat_names = f.readline().strip().split('=')[1].split(',')
database = f.readline().strip().split('=')[1]
host = f.readline().strip().split('=')[1]
user = f.readline().strip().split('=')[1]
password = f.readline().strip().split('=')[1]
port = f.readline().strip().split('=')[1]
source_schema = f.readline().strip().split('=')[1]
source_table = f.readline().strip().split('=')[1]
target_schema = f.readline().strip().split('=')[1]
target_table = f.readline().strip().split('=')[1]
horizon = int(f.readline().strip().split('=')[1])
categorical_threshold = int(f.readline().strip().split('=')[1])
fill_strategy = f.readline().strip().split('=')[1]

# connecting to the database
conn = psycopg2.connect(database=database,
                        host=host,
                        user=user,
                        password=password,
                        port=port)

# create pandas dataframe from columns names
cursor = conn.cursor()
cursor.execute("SELECT * FROM "
               "information_schema.columns "
               "WHERE table_schema = %s "
               "AND table_name   = %s;",
               (target_schema, target_table))
ls = cursor.fetchall()
columns = []
for item in ls:
    columns.append(item[3])
initial_df = pd.DataFrame(columns=columns[:-1])

# fill pandas dataframe with data from database with right horizon
cursor = conn.cursor()
sql_query = "SELECT * FROM " + target_schema + '.' + target_table + ';'  # ' where (current_date - load_date) > ' +str(horizon)+';'
cursor.execute(sql_query)
ls = cursor.fetchall()
size = len(ls)
for item in ls:
    data = item[:-1]
    initial_df = pd.concat([initial_df,
                            pd.DataFrame([data], columns=columns[:-1])],
                           ignore_index=True)
# processing
score = learn(initial_df, 'medv',['town'],0.33)

time = time.process_time() - start
date = datetime.today().strftime('%Y-%m-%d')
conn = psycopg2.connect(database=database,
                        host=host,
                        user=user,
                        password=password,
                        port=port)
cursor = conn.cursor()
sql_query = "insert into learn_info (size, process_time,process_date,score) values (%s,%s,%s,%s);"
cursor.execute(sql_query,[size,time,date,score])
conn.commit()
conn.close()