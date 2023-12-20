# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "********************")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "************************************")
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_claims = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/claims/")
# df_claims.show()
df_disease = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/disease/")
# df_disease.show()
df_hospital = spark.read.options(inferSchema='True', header='True').parquet("s3://capstone-proj/input-data/hospital/")
# df_hospital.show()
df_subscriber = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/subscriber/")
# df_subscriber.show()
df_patient = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/patient/")
# df_patient.show()
df_groupsubgp = spark.read.options(inferSchema='True', header='True').parquet("s3://capstone-proj/input-data/group_subgp/")
# df_groupsubgp.show()
df_group = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/group/")
# df_group.show()
df_subgroup = spark.read.options(inferSchema='True', header='True', delimiter=',').csv("s3://capstone-proj/input-data/subgroup/")
# df_subgroup.show()


# COMMAND ----------

#1
from pyspark.sql.functions import *
print("Disease with MAX claims:")
df = df_claims.groupBy("disease_name").agg(count("claim_id").alias("MaxClaims"))
df1 = df.filter(col("MaxClaims") == df.select(max("MaxClaims")).first()[0])
df1.show()

# COMMAND ----------

df1.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.disease_with_max_claims")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase1/")\
    .option("user", "*****")\
    .option("password", "********")\
    .mode("overwrite")\
    .save()

# COMMAND ----------

#2
print("Subscriber having age less than 30 and subscribed to a subgroup:")
df = df_subscriber.withColumn("Age", datediff(current_date(), col("Birth_date")) / 365.25).orderBy(asc("Age"))
df2 = df.select("sub_id","first_name","last_name",round("Age", 0).alias("Age"),"Subgrp_id").filter((df.Age >= 30) & (df.Subgrp_id != "NA"))
df2.show()

# COMMAND ----------

df2.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.subscr_below30_subgp")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase2/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#3
print("Group that has maximum subgroups:")
df = df_groupsubgp.groupBy("Grp_Id").agg(count("SubGrp_ID").alias("TotSubgroup")).orderBy(asc("TotSubgroup"))
df3 = df.filter(col("TotSubgroup") == df.select(max("TotSubgroup")).first()[0])
df3.show()

# COMMAND ----------

df3.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.group_with_max_subgrp")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase3/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#4
print("Hospital which serve most number of patients")
df = df_patient.groupBy("hospital_id").agg(count("patient_id").alias("TotPatient")).orderBy(desc("TotPatient"))
df4 = df.filter(col("TotPatient") == df.select(max("TotPatient")).first()[0])
df4.show()

# COMMAND ----------

df4.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.hospital_with_max_patient")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase4/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#5
print("Subgroup subscribed most number of times:")
df = df_subscriber.groupBy("Subgrp_id").agg(count("sub_id").alias("TotSubscribers")).orderBy(desc("TotSubscribers"))
df5 = df.filter(col("TotSubscribers") == df.select(max("TotSubscribers")).first()[0])
df5.show()

# COMMAND ----------

df5.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.popular_subgrp")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase5/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#6
print("Total number of claims which were rejected:")
df = df_claims.groupBy("claim_or_rejected").agg(count("claim_or_rejected").alias("TotClaimRejected"))
df6 = df.filter(col("claim_or_rejected") == 'N')
df6.show()

# COMMAND ----------

df6.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.claims_rejected")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase6/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#7
print("City withe most claims:")
df = df_claims.join(df_patient, (df_claims.patient_id) == (df_patient.Patient_id), "inner")
dfclaimcount = df.select("city","SUB_ID").groupBy("city").agg(count("SUB_ID").alias("TotClaims")).orderBy(desc("TotClaims"))
df7 = dfclaimcount.filter(col("TotClaims") == dfclaimcount.select(max("TotClaims")).first()[0])
df7.show()

# COMMAND ----------

df7.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.city_with_maxclaims")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase7/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#9
from pyspark.sql.functions import *
print("Average monthly premium subscriber pay to insurance company:")
df9 = df_group.select(round(avg("premium_written"),2).alias("AveragePremium"))
df9.show()

# COMMAND ----------

df9.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.avg_monthly_premium")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase9/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#10
print("Most profitable group:")
df10 = df_group.select("Grp_Id", "Grp_Name", "premium_written").filter(col("premium_written") == df_group.select(max("premium_written")).first()[0])
df10.show()

# COMMAND ----------

df10.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.most_profitable_gp")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase10/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()

# COMMAND ----------

#11
print("List of patients below age of 18 who are admit for cancer: ")
df = df_patient.withColumn("Age", datediff(current_date(), col("patient_birth_date")) / 365.25).orderBy(asc("Age"))
df11 = df.select("patient_name", round("Age", 0).alias("Age"), "disease_name").filter((df.Age <= 18) & (df.disease_name.like("%cancer%")))
df11.show()
df11.printSchema()

# COMMAND ----------

df11.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.202126886237.us-east-1.redshift-serverless.amazonaws.com:5439/sample_data_dev")\
    .option("dbtable", "project_output.patient_below18_with cancer")\
    .option("aws_iam_role", "arn:aws:iam::202126886237:role/redshift_admin")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("tempdir", "s3a://capstone-proj/redshift-dir/usecase11/") \
    .option("user", "*****") \
    .option("password", "********") \
    .mode("overwrite")\
    .save()
