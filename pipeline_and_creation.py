from pyspark.sql import SparkSession

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
         user="your_supabase_user",
         password="your_supabase_password",
         url="jdbc:postgresql://aws-0-eu-west-2.pooler.supabase.com:5432/postgres",
         query=test_query
         ) \
.load()

df.write.mode("overwrite").parquet("test/temp_member_spend.parquet")