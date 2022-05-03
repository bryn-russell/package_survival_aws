# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Get data needed to create survival data
# MAGIC 
# MAGIC ## Improvements
# MAGIC - Remove any explicit date references 

# COMMAND ----------

pip install s3fs

# COMMAND ----------

import datetime as datetime
import pyspark.sql.functions as psf
from pyspark.sql.functions import when
from pyspark.sql.window import Window
# import pyspark.sql.functions as func

# define the time period you which to look at


# COMMAND ----------

start = datetime.date(2021,7,1)
# end = datetime.now()
end = datetime.date(2022,3,16)
delta = datetime.timedelta(days=1)


fileinPath_products = []
path_products = 's3://atuk-prod-emr-refined-data/core/customer/dealer-products/1-0-0/'

while start <= end:
    fileinPath_products.append("{0}/year={1:%Y}/month={1:%m}/day={1:%d}/date={1:%Y%m%d}".format(path_products, start))
    start += delta
    
print(path_products)


products_raw = spark.read.format("parquet").option("basePath",path_products).load(fileinPath_products)

# COMMAND ----------

# print(fileinPath_products)

# display(products)

# COMMAND ----------

products_all = products_raw.select('did', 'core_product_code', 'date') \
  .withColumn('package_level', when(products_raw.core_product_code == "ADVANTAGEPLUS", 6)
             .when(products_raw.core_product_code == "WDPS", 5)
             .when(products_raw.core_product_code == "PROMOTED", 4)
             .when(products_raw.core_product_code == "WMPS", 3)
             .when(products_raw.core_product_code == "WSSO", 2)
             .when(products_raw.core_product_code == "FPS", 1)
             ) 

products = products_all.filter(products_all.package_level.isNotNull()) \
  .select("did","date","package_level") \
  .sort("date")

products_grouped = products.groupBy("did", "date").max('package_level')
# print(products.count() )

# display(products)


# COMMAND ----------

products_lag = products_grouped.withColumn('prev_day_package_level',
                                  psf.lag(products_grouped['max(package_level)'])
                                  .over(Window.partitionBy("did").orderBy("date"))
                                 )

products_difference = products_lag.withColumn('difference', products_lag["max(package_level)"] - products_lag["prev_day_package_level"])


products_filtered = products_difference.filter(products_difference["difference"] != 0)
oneDid = products_filtered.filter(products_difference['did'] == '137034')


# COMMAND ----------

# display(oneDid)

# COMMAND ----------

# print(products_filtered.count())

# COMMAND ----------

products_filtered_pd = products_filtered.toPandas()

outfilePath_csv = "s3://atuk-prod-emr-user-data/users/Bryn.Russell/220307_Package_survival/package_level_data.csv"

# products_filtered.repartition(1).write.mode("overwrite").csv(outfilePath_csv, header= 'true')
products_filtered_pd.to_csv(outfilePath_csv)


# COMMAND ----------


