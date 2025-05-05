# Import necessary libraries
import pandas as pd  # Importing pandas for data manipulation and analysis
import pyodbc       # Importing pyodbc for connecting to SQL Server
import cx_Oracle     # Importing cx_Oracle for connecting to Oracle databases

# Step 1: Import data from Excel
def import_from_excel(file_path):
    """
    This function reads data from an Excel file and returns it as a DataFrame.
    
    Parameters:
    file_path (str): The path to the Excel file to be read.
    
    Returns:
    DataFrame: A pandas DataFrame containing the data from the Excel file.
    """
    df_excel = pd.read_excel(file_path)  # Read the Excel file into a DataFrame
    return df_excel  # Return the DataFrame

# Step 2: Import data from SQL Server
def import_from_sql_server(server, database, query):
    """
    Connects to the SQL Server database and retrieves data based on the provided SQL query.

    Parameters:
    server (str): The name or IP address of the SQL Server.
    database (str): The name of the database to connect to.
    query (str): The SQL query to execute for data retrieval.

    Returns:
    DataFrame: A pandas DataFrame containing the result of the query.
    """
    # Establish a connection to the SQL Server database
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
    
    # Execute the SQL query and store the result in a DataFrame
    df_sql = pd.read_sql_query(query, conn)
    
    # Close the database connection
    conn.close()
    
    # Return the DataFrame containing the retrieved data
    return df_sql

# Step 3: Import data from Oracle
def import_from_oracle(username, password, dsn, query):
    """
    Connects to the Oracle database and retrieves data based on the provided SQL query.

    Parameters:
    username (str): Oracle database username
    password (str): Oracle database password
    dsn (str): Data Source Name (connection string)
    query (str): SQL query to execute

    Returns:
    DataFrame: A pandas DataFrame containing the result of the query
    """
    # Establish a connection to the Oracle database
    conn = cx_Oracle.connect(username, password, dsn)
    
    # Execute the SQL query and load the result into a DataFrame
    df_oracle = pd.read_sql_query(query, conn)
    
    # Close the database connection
    conn.close()
    
    # Return the DataFrame
    return df_oracle

# Step 4: Load data into the target system (e.g., a new database)
def load_to_target(df, target_conn):
    """
    This function loads a DataFrame into a target database table.
    
    Parameters:
    df (DataFrame): The DataFrame to load into the target database.
    target_conn: The connection object for the target database.
    """
    # Load the DataFrame into the target table, replacing it if it already exists
    df.to_sql('target_table', target_conn, if_exists='replace', index=False)

# Main function to execute the process
def main():
    # Import data from Excel
    excel_data = import_from_excel('data.xlsx')  # Specify the path to your Excel file
    
    # Import data from SQL Server
    sql_data = import_from_sql_server('localhost', 'SampleDB', 'SELECT * FROM Employees')  # Adjust the query as needed
    
    # Import data from Oracle
    oracle_data = import_from_oracle('user', 'password', 'localhost/XE', 'SELECT * FROM Customers')  # Adjust credentials and query
    
    # Combine data (optional)
    combined_data = pd.concat([excel_data, sql_data, oracle_data], ignore_index=True)  # Combine all DataFrames into one
    
    # Load data into the target system
    target_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=TargetDB;Trusted_Connection=yes;')  # Connection to target database
    load_to_target(combined_data, target_conn)  # Load the combined data into the target table
    target_conn.close()  # Close the connection

# Entry point of the script
if __name__ == "__main__":
    main()  # Execute the main function
