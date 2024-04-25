import pandas as pd

### test for 'test connection with supabase' section ###

df = pd.read_parquet('test/temp_member_spend.parquet')

print(df.head())

### test for 'retrieving a whole table' section ###

# df = pd.read_parquet('test/temp_members.parquet')

# print(df.head())

### test for 'loading data into postgres database' section ###
 
# to test look at the tables in the database.

### test for 'the pipeline' section ###
 
# to test look at the tables in the database.

### test for 'ETL pipeline' section ###
 
# to test look at the tables in the database.