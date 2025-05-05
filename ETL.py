""" Data Visualization from Extraction Transformation and Loading (ETL) Process"""

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

import matplotlib.pyplot as plt

categories = aggregate_data[0].index.to_list()

def plot_aggreagate_data(df, categories):
    plt.figure(figsize=(12, 6))
    plt.bar(x=categories,height=df.values.ravel())
    plt.title('Total Revenue by Product')
    plt.xlabel('Product')
    plt.ylabel('Total Revenue')

for df in aggregate_data:
    plot_aggreagate_data(df, categories)
