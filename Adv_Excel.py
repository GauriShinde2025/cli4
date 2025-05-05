"""Data Analysis and Visualization using Advanced Excel."""

# Import libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Remove duplicates
sales_df.drop_duplicates(inplace=True)

# Handle missing values (e.g., fill with 0)
sales_df["Quantity"].fillna(0, inplace=True)

# Convert OrderDate to datetime
sales_df["OrderDate"] = pd.to_datetime(sales_df["OrderDate"])

sales_df = pd.merge(sales_df, customers_df, on="CustomerID", how="left")

# VLOOKUP: Merge Product Price into Sales Data
sales_df = pd.merge(sales_df, products_df[["Product", "Price"]], on="Product", how="left")

# SUMIF: Calculate Total Revenue (Quantity * Price)
sales_df["TotalRevenue"] = sales_df["Quantity"] * sales_df["Price"]

print("\nMerged Data with Revenue:")
display(sales_df.head())

# Create a PivotTable: Total Revenue by Region and Product
pivot_table = pd.pivot_table(
    sales_df,
    values="TotalRevenue",
    index="Region",
    columns="Product",
    aggfunc="sum",
    fill_value=0
)

print("\nPivotTable (Revenue by Region & Product):")
display(pivot_table)

# Bar Chart: Total Revenue by Product
plt.figure(figsize=(10, 6))
sns.barplot(data=sales_df, x="Product", y="TotalRevenue", estimator=sum, ci=None)
plt.title("Total Revenue by Product (Excel-like Bar Chart)")
plt.xlabel("Product")
plt.ylabel("Total Revenue ($)")
plt.show()

# Line Chart: Monthly Sales Trend
sales_df["Month"] = sales_df["OrderDate"].dt.month_name()
monthly_sales = sales_df.groupby("Month")["TotalRevenue"].sum().reset_index()

plt.figure(figsize=(10, 6))
sns.lineplot(data=monthly_sales, x="Month", y="TotalRevenue", marker="o")
plt.title("Monthly Sales Trend (Excel-like Line Chart)")
plt.xlabel("Month")
plt.ylabel("Total Revenue ($)")
plt.show()
