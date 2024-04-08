# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pymssql
from datetime import datetime, timedelta

# %%
task1_t1 = pd.DataFrame([{'Id':id, 'Quantity':np.random.randint(10, 100)} for id in range(1000)])
task1_t2 = task1_t1.copy()
task1_t2['Quantity'] = task1_t2['Quantity'].apply(lambda q: int(q * np.random.randint(110, 150)/100))
print(task1_t1.shape, task1_t2.shape)
print(task1_t1.head(2))
print(task1_t2.head(2))
plt.hist(100 - (task1_t1['Quantity'] / task1_t2['Quantity'] * 100))
plt.ylabel('количество записей')
plt.xlabel('доля прироста')
plt.title('распределение прироста в поле Quantity')
plt.show()



# %%
with pymssql.connect(server='95.165.31.67:1433',
                     user='sa',
                     password='Pass1sF@ke',
                     database='test') as conn:
    with conn.cursor() as cursor:
        for t_name, data in zip(['task1_t1', 'task1_t2'], [task1_t1, task1_t2]):
            print(t_name, 'processing', len(data), 'rows')
            query = f'''insert into dbo.{t_name} (Quantity) values (%d)'''
            cursor.executemany(query, list(data['Quantity'].astype(str).values))
            conn.commit()

# %%
date_start = datetime(2020, 1, 1)
task2_t1 = pd.DataFrame([{'Id':id, 'Quantity':np.random.randint(10, 100), 'Data':date_start + timedelta(days=id)} for id in range(1000)])
print(task2_t1.shape)
task2_t1.head(2)

# %%
with pymssql.connect(server='95.165.31.67:1433',
                     user='sa',
                     password='Pass1sF@ke',
                     database='test') as conn:
    with conn.cursor() as cursor:
        query = f'''insert into dbo.task2_t1 (Quantity, Data) values (%d, %s)'''
        cursor.executemany(query, list(map(tuple, task2_t1[['Quantity', 'Data']].astype(str).values)))
        conn.commit()

# %%
task3_t1 = pd.DataFrame([{'Id':id, 'Quantity':np.random.randint(10, 100)} for id in range(1000)])
print(task3_t1.shape)
task3_t1.head(2)

# %%
with pymssql.connect(server='95.165.31.67:1433',
                     user='sa',
                     password='Pass1sF@ke',
                     database='test') as conn:
    with conn.cursor() as cursor:
        query = f'''insert into dbo.task3_t1 (Quantity) values (%d)'''
        cursor.executemany(query, list(task3_t1['Quantity'].astype(str).values))
        conn.commit()

# %%



