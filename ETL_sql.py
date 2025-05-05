"""Perform the Extraction Transformation and Loading (ETL) process to construct the database in the Sql server / Power BI."""

import pandas as pd

oracle_df = pd.read_csv('large_oracle_data.csv')
sql_df = pd.read_csv('large_sql_data.csv')
sales_df = pd.read_excel('large_sales_data.xlsx')

cols = oracle_df.columns
data = [oracle_df, sql_df, sales_df]

oracle_df.head(), sql_df.head(), sales_df.head()

# Transformation

pd.DataFrame({'oracle_df':oracle_df.isna().sum().values,
          'sql_df':sql_df.isna().sum().values,
          'sales_df':sales_df.isna().sum().values},index=oracle_df.columns)

# Filtering

for df in data:
    df.drop('order_id',axis=1,inplace=True)

for df in data:
    print(df.head(),'\n\n')

# Aggrigating 

aggregate_data = []

for df in data:
    aggregate_data.append(df.drop('order_date',axis=1).groupby(by='product_category').sum())

for df in aggregate_data:
    print(df.head(),'\n\n')

from sqlalchemy import create_engine
engine = create_engine(
    "mysql+mysqlconnector://root:SQLpassword@localhost/new_database"
)

df.to_sql('Sales', engine, if_exists='replace', index=False)
