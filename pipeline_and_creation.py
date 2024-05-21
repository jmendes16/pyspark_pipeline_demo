import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# loading in credentials
load_dotenv()

SUPABASE_USER=os.environ.get("SUPABASE_USER")
SUPABASE_PASSWORD=os.environ.get("SUPABASE_PASSWORD")
LOCAL_USER=os.environ.get("LOCAL_USER")
LOCAL_PASSWORD=os.environ.get("LOCAL_PASSWORD")

spark = SparkSession.builder.master("local[1]").appName("fashion_forward_demo").getOrCreate()

### test connection with supabase ###
# make sure you have looked at the README.md file and setup your environment correctly.
# you will need to put in your supabase user and password, this can be found in 
# Project Settings>Database. Your host may also be different.  

test_query="""WITH member_spend AS 
                (SELECT member_id, sum(price) AS total_spend
                FROM (SELECT * FROM q2sales UNION SELECT * FROM q3sales) AS all_sales
                LEFT JOIN products ON product_id = product_code
                GROUP BY member_id)
            SELECT firstname, surname, total_spend
            FROM member_spend LEFT JOIN members USING(member_id)
            ORDER BY 3 DESC"""

df = spark.read \
.format("jdbc") \
.options(driver="org.postgresql.Driver",
         user=SUPABASE_USER,
         password=SUPABASE_PASSWORD,
         url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
         query=test_query
         ) \
.load()

df.write.mode("overwrite").parquet("test/temp_member_spend.parquet")

### retrieving a whole table ###
# remember to comment out previous section

# df = spark.read \
# .format("jdbc") \
# .options(driver="org.postgresql.Driver",
#          user=SUPABASE_USER,
#          password=SUPABASE_PASSWORD,
#          url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#          dbtable="members"
#          ) \
# .load()

# df.write.mode("overwrite").parquet("test/temp_members.parquet")

### loading data into postgres database ###
# remember to comment out previous section
# in this example I will be loading the data into a postgres database that I am running
# locally using PgAdmin you may need to change the parameters to match your settings.
# alternatively you can load it back into supabase with a different name or try sending it
# to a different application.  
# to test look at the tables in the database.

# df = spark.read \
# .format("jdbc") \
# .options(driver="org.postgresql.Driver",
#          user=SUPABASE_USER,
#          password=SUPABASE_PASSWORD,
#          url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#          dbtable="members"
#          ) \
# .load()

# # this may be a situation where you might use "append" instead of "overwrite"
# df.write.mode("overwrite") \
# .format("jdbc") \
# .options(driver="org.postgresql.Driver",
#         user=LOCAL_USER,
#         password=LOCAL_PASSWORD,
#         url="jdbc:postgresql://localhost:5432/fashion_forward",
#         dbtable="members_spark"
# ) \
# .save()

### the pipeline ###
# remember to comment out previous section
# to test look at the tables in the database.

# # get a list of table names from the database
# df_tablenames = spark.read \
# .format("jdbc") \
# .options(driver="org.postgresql.Driver",
#          user=SUPABASE_USER,
#          password=SUPABASE_PASSWORD,
#          url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#          query="""SELECT table_name FROM information_schema.tables 
#                 WHERE table_schema = 'public'"""
#          ) \
# .load()

# # iterate through each table in the database extract it and load it into target database
# for table_name in df_tablenames.select("table_name").collect():
#     df = spark.read.format("jdbc") \
#         .options(driver="org.postgresql.Driver",
#                 user=SUPABASE_USER,
#                 password=SUPABASE_PASSWORD,
#                 url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#                 dbtable=table_name.table_name
#                 ) \
#         .load()

#     df.write.mode("overwrite").format("jdbc") \
#         .options(driver="org.postgresql.Driver",
#         user=LOCAL_USER,
#         password=LOCAL_PASSWORD,
#         url="jdbc:postgresql://localhost:5432/fashion_forward",
#         dbtable="spark_" + table_name.table_name
#         ) \
#         .save()

### ETL pipeline ###
# remember to comment out previous section
# to test look at the tables in the database.

# get a list of table names from the database
# df_tablenames = spark.read \
# .format("jdbc") \
# .options(driver="org.postgresql.Driver",
#          user=SUPABASE_USER,
#          password=SUPABASE_PASSWORD,
#          url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#          dbtable="information_schema.tables"
#          ) \
# .load().filter("table_schema='public'").select("table_name")

# # iterate through each table in the database and extract it into the spark session
# for table_name in df_tablenames.select("table_name").collect():
#     df = spark.read.format("jdbc") \
#         .options(driver="org.postgresql.Driver",
#                 user=SUPABASE_USER,
#                 password=SUPABASE_PASSWORD,
#                 url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
#                 dbtable=table_name.table_name
#                 ) \
#         .load().createTempView(table_name.table_name)

# # create queries to transform the data we would like to output
# state_deliveries="""
#         SELECT state, count(*)
#         FROM (SELECT * FROM q2sales UNION SELECT * FROM q3sales) AS all_sales
#         JOIN delivery USING(transaction_id)
#         GROUP BY state
#         ORDER BY 2 DESC;
#         """

# member_spend="""
#             WITH member_spend AS 
#                 (SELECT member_id, sum(price) AS total_spend
#                 FROM (SELECT * FROM q2sales UNION SELECT * FROM q3sales) AS all_sales
#                 LEFT JOIN products ON product_id = product_code
#                 GROUP BY member_id)
#             SELECT firstname, surname, total_spend
#             FROM member_spend LEFT JOIN members USING(member_id)
#             ORDER BY 3 DESC
#             """

# state_spend="""
#         SELECT state, sum(price)
#         FROM (SELECT * FROM q2sales UNION SELECT * FROM q3sales) AS all_sales
#         JOIN delivery USING(transaction_id)
#         LEFT JOIN products ON product_id = product_code
#         GROUP BY state
#         ORDER BY 2 DESC;
# """

# third_items="""
#         WITH numbered_item_orders AS
#                 (SELECT firstname, surname, line, category, color, price, row_number() OVER 
#                 (PARTITION BY firstname, surname ORDER BY sale_date, sale_id) AS individual_item_order_number
#                 FROM (SELECT * FROM q2sales UNION SELECT * FROM q3sales) AS all_sales
#                 JOIN delivery USING(transaction_id)
#                 LEFT JOIN products ON product_id = product_code)
#         SELECT * 
#         FROM numbered_item_orders
#         WHERE individual_item_order_number = 3;
# """

# queries={"state_deliveries":state_deliveries, 
#          "member_spend":member_spend, 
#          "state_spend":state_spend, 
#          "third_items":third_items}

# # iterate through queries loading into target database
# for query in queries:
#     df=spark.sql(queries[query])

#     df.write.mode("overwrite").format("jdbc") \
#         .options(driver="org.postgresql.Driver",
#         user=LOCAL_USER,
#         password=LOCAL_PASSWORD,
#         url="jdbc:postgresql://localhost:5432/fashion_forward",
#         dbtable=query
#         ) \
#         .save()    
