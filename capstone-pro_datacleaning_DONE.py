# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "********************")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "************************************")
from pyspark.sql.types import *

# COMMAND ----------

claimsDF = spark.read.json("s3://capstone-proj/raw-data/claims/claims.json")
claimsDF2 = claimsDF.dropDuplicates()
claimsDF2.show()

# COMMAND ----------


diseaseDF = spark.read.options(inferSchema='True', header='True').csv("s3://capstone-proj/raw-data/disease/disease.csv")
diseaseDF1 = diseaseDF.dropDuplicates()
diseaseDF2 = diseaseDF1.fillna("NA", subset=["SubGrpID","Disease_name"]).fillna(0, subset=["Disease_ID"])
diseaseDF2.show()


# COMMAND ----------


groupDF = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/raw-data/group")
groupDF_fill = groupDF.fillna("NA", subset=["Country", "Grp_Id","Grp_Name","Grp_Type","city"]).fillna(0, subset=["premium_written","zipcode","year"])
groupDF_replace = groupDF_fill.withColumn('city', groupDF_fill.city).replace("Delhi","New Delhi").replace('Gurgaon','Gurugram').replace('Bangalore','Bengaluru')
groupDF2 = groupDF_replace.dropDuplicates()
groupDF2.show(truncate=False)


# COMMAND ----------

grpsubgrpDF = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/raw-data/grpsubgrp")
grpsubgrpDF1 = grpsubgrpDF.dropDuplicates()
grpsubgrpDF2 = grpsubgrpDF1.fillna("NA", subset=["SubGrp_ID","Grp_Id"])
grpsubgrpDF2.show()

# COMMAND ----------


schema = StructType().add("Hospital_id",StringType(),True).add("Hospital_name",StringType(),True).add("City",StringType(),True).add("State",StringType(),True).add("Country",StringType(),True)
hospitalDF = spark.read.options(header='False', delimiter=',').schema(schema).csv("s3://capstone-proj/raw-data/hospital")
hospitalDF_replace = hospitalDF.withColumn("City", hospitalDF.City).replace("Delhi","New Delhi").replace("Gurgaon","Gurugram")
hospitalDF2 = hospitalDF_replace.dropDuplicates()
hospitalDF2.show()

# COMMAND ----------

patientDF = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/raw-data/patientrecords")
patientDF1 = patientDF.fillna("NA", subset=["Patient_name","patient_gender","disease_name","city","hospital_id"]).fillna(0, subset=["Patient_id","patient_phone"]).fillna("19000101", subset=["patient_birth_date"])
patientDF2 = patientDF1.dropDuplicates()
patientDF2.show()

# COMMAND ----------

subgrpDF = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/raw-data/subgroup")
subgrpDF2 = subgrpDF.dropDuplicates()
subgrpDF2.show()

# COMMAND ----------

subscriberDF = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/raw-data/subscriber")
subscriberDF_fill = subscriberDF.fillna("NA", subset=["sub_id","first_name","last_name","Street","Gender","Country","City","Subgrp_id","Elig_ind"]).fillna("19000101", subset=["Subgrp_id","eff_date","term_date"]).fillna(0, subset=["Phone", "ZipCode"])
subscriberDF2 = subscriberDF_fill.dropDuplicates()
subscriberDF2.show()

# COMMAND ----------

# WRITE CLEAN DATA INTO S3

claimsDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/claims/")
diseaseDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/disease/")
grpsubgrpDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("parquet").save("s3://capstone-proj/input-data/group_subgp/")
groupDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/group/")
hospitalDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("parquet").save("s3://capstone-proj/input-data/hospital/")
patientDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/patient/")
subgrpDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/subgroup/")
subscriberDF2.write.mode("overwrite").options(inferSchema='True', header='True').format("csv").save("s3://capstone-proj/input-data/subscriber/")

