# pyspark_pipeline_demo
Code used for demonstrating how you can use pyspark to create a datapipeline

### Python Script Instruction ###

# these demonstrations are separated into sections within a script.  
# for the purposes of instructional flow please comment out previous sections before moving on to 
# the next.  You can then re-run the same code in the terminal. 
# please start with pipeline_and_creation.py, making sure to pay attention to the instructions
# at the top of each section. temp_file_reads.py is there as a way to help you check it is working
# as expected.  

### Installation Instructions ###

# make sure you have java installed on your device, this can be installed from the oracle website
# download spark from https://spark.apache.org/downloads.html you extract into a folder c:/Spark
# download hadoop3 and install to a folder c:/hadoop
# you may need to use your user space if admin access is required to create folders on your c drive
# create environment variables and add paths to spark and hadoop bin.
# download jdbc postgres driver from https://jdbc.postgresql.org/download/ and add it to the jars folder
# of your Spark install.  
# you can also download and install PgAdmin from https://www.enterprisedb.com/downloads/postgres-postgresql-downloads 
# make sure you remember your password as you will need it to access your local database.  
# you can create a database in PgAdmin by going to object>create>database
# run pip install pyspark pyarrow pandas in your python virtual environment