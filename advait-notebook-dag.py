from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, xxhash64, concat, current_date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.utils import AnalysisException

# take 3 on github upload

spark = SparkSession \
        .builder \
        .appName("DeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
spark.sparkContext.addPyFile("s3://advait-landingbucket-batch08/dependencies/delta-core_2.12-0.8.0.jar")

from delta import *

class App:
    def __init__(self, app_name, app_config_path):
        self.app_name = app_name
        self.app_config_path = app_config_path
        self.app_config = self.read_config()

    def read_config(self):
        return spark.read.option("multiLine", True).json(self.app_config_path)

    def read_datasets(self):
        datasets = self.app_config.select(col("ingest-dataset.datasets")).collect()[0][0]
        source_path = self.app_config.select(col("ingest-dataset.source.data-location")).collect()[0][0]
        destination_path = self.app_config.select(col("ingest-dataset.destination.data-location")).collect()[0][0]
        file_format = self.app_config.select(col("ingest-dataset.source.file-format")).collect()[0][0]
        partition_by = self.app_config.select(col("transformation-dataset.partition-cols")).collect()[0][0]
        staging_path = self.app_config.select(col("lookup-dataset.data-location")).collect()[0][0]

        dfs = {}
        for i, dataset_name in enumerate(datasets):
            try:
                df = spark.read.format(file_format).load(source_path + dataset_name + '.' + file_format)
            except AnalysisException as e:
                datasets = [name for name in datasets if name!=dataset_name]
                continue

            dfs[dataset_name] = df
        dfs_original = dfs.copy()

        return datasets, source_path, destination_path, file_format, partition_by, staging_path, dfs, dfs_original


    def write_data(self, dfs, datasets, destination_path, file_format, partition_by=None):
        if partition_by is not None:
            for i, (df_name, df) in enumerate(dfs.items()):
                df.write.format(file_format) \
                .partitionBy(partition_by) \
                .mode('overwrite').save(destination_path + datasets[i])
        else:
            for i, (df_name, df) in enumerate(dfs.items()):
                df.write.format(file_format) \
                .mode('overwrite').save(destination_path + datasets[i])

    def casting(self, dfs):
        casting_col = self.app_config.select(col("transformation-dataset.transformation-cols")).collect()[0][0].asDict()
        for col_name, col_type in casting_col.items():
            if col_type.startswith('Deci'):
                precision, scale = col_type.split(',')
                total_digit = 7 + int(scale)
                col_type = 'decimal({0},{1})'.format(total_digit, scale)

            for i, (df_name, df) in enumerate(dfs.items()):
                if col_name in df.columns:
                    df = df.withColumn(col_name, df[col_name].cast(col_type))
            dfs[df_name] = df
        return dfs

    def masking(self, dfs):
        masking_col = self.app_config.select(col("transformation-dataset.masking-cols")).collect()[0][0]
        for i, (df_name, df) in enumerate(dfs.items()):
            for col_name in masking_col:
                if col_name in df.columns:
                    df = df.withColumn(col_name, sha2(df[col_name], 256))
            dfs[df_name] = df
        return dfs
    
    def derive_source_df(self, dfs, dfs_original, datasets):
        source_df = {}
        for dataset in datasets:
            if dataset == "actives":
                # transformed data
                tf = dfs[dataset]
                # original data
                df = dfs_original[dataset]
                # select desired columns
                #pii_columns = self.app_config.select(col("lookup-dataset.pii-cols")).collect()[0][0]
                tf = tf.selectExpr("advertising_id as masked_advertising_id", "user_id as masked_user_id", "date as start_date", "timestamp")
                df = df.selectExpr("advertising_id", "user_id", "date", "timestamp")
                # join table on timestamp and delete unneccessary columns
                source_df[dataset] = df.join(tf, on="timestamp", how="inner").drop(col("timestamp")).drop(col("date"))
                source_df[dataset] = source_df[dataset].withColumn("end_date", lit(None).cast("date")).withColumn("flag", lit("Active"))
            else:
                # transformed data
                tf = dfs[dataset]
                # original data
                df = dfs_original[dataset]
                # select desired columns
                #pii_columns = self.app_config.select(col("lookup-dataset.pii-cols")).collect()[0][0]
                tf = tf.selectExpr("advertising_id as masked_advertising_id", "date as start_date", "record_timestamp")
                df = df.selectExpr("advertising_id", "date", "record_timestamp")
                # join table on timestamp and delete unneccessary columns
                source_df[dataset] = df.join(tf, on="record_timestamp", how="inner").drop(col("record_timestamp")).drop(col("date"))
                source_df[dataset] = source_df[dataset].withColumn("end_date", lit(None).cast("date")).withColumn("flag", lit("Active"))
        return source_df
    
    def lookup_dataset(self, source_df, datasets):
        lookup_location = "s3://advait-stagingbucket-batch08/lookup_dataset/"
        # Go through all the datasets
        for dataset in datasets:
            if dataset == 'actives':
                # read delta table 
                try:
                    # read delta table in dataframe format for further pd operations
                    delta_table = spark.read.format('delta').load(lookup_location+dataset)
                except:
                    # creating schema for source table and reading data
                    schema = StructType([
                                          StructField('advertising_id', StringType(), True),
                                          StructField('user_id', StringType(), True),
                                          StructField('masked_advertising_id', StringType(), True),
                                          StructField('masked_user_id', StringType(), True),
                                          StructField('start_date', TimestampType(), True),
                                          StructField('end_date', TimestampType(), True),
                                          StructField('flag', StringType(), True)
                                          ])
                    delta_table = spark.createDataFrame(data=[], schema=schema)
                    delta_table.write.format('delta').mode("overwrite").option("overwriteSchema", "true").save(lookup_location+dataset)
                    delta_table = spark.read.format('delta').load(lookup_location+dataset)
                
                # Join delta_table and source_table
                source = source_df[dataset]
                joinDF = source.join(delta_table,(source.advertising_id==delta_table.advertising_id) & \
                                        (delta_table.flag=="Active"),"leftouter") \
                                 .select(source["*"], \
                                        delta_table.advertising_id.alias("delta_advertising_id"), \
                                        delta_table.masked_advertising_id.alias("delta_masked_advertising_id"), \
                                        delta_table.user_id.alias("delta_user_id"), \
                                        delta_table.masked_user_id.alias("delta_masked_user_id"))

                # Filtering the new records only
                filter_table = joinDF.filter(xxhash64(joinDF.advertising_id,joinDF.user_id) 
                                             != xxhash64(joinDF.delta_advertising_id,joinDF.delta_user_id))
                
                # Merging the key columns
                merge_table = filter_table.withColumn("MERGE_KEY",concat(filter_table.advertising_id,filter_table.user_id))
                
                # Dummy table to insert null key for columns to be updated
                dummy_table = filter_table.filter("delta_advertising_id is not null").withColumn("MERGE_KEY",lit(None))
                
                # Combining dummy table and merge table
                scd_table = merge_table.union(dummy_table)
                
                
                # read delta table in delta format to run .merge method
                Delta_table = DeltaTable.forPath(spark, lookup_location+dataset)
                
                Delta_table.alias("delta").merge(
                    source = scd_table.alias("source"),
                    condition = "concat(delta.advertising_id,delta.user_id) = source.MERGE_KEY and delta.flag='Active'"
                    ).whenMatchedUpdate(set =
                        { 
                            "flag" : "'Inactive'",
                            "end_date":"current_date"
                        }
                    ).whenNotMatchedInsert(values =
                        {
                        "advertising_id" : "source.advertising_id",
                        "user_id" : "source.user_id",
                        "masked_advertising_id" : "source.masked_advertising_id",
                        "masked_user_id" : "source.masked_user_id",
                        "flag" : "'Active'",
                        "start_date" : "current_date",
                        "end_date": "'None'"
                        }
                    ).execute()
            else:
                try:
                    # read delta table in dataframe format for further pd operations
                    delta_table = spark.read.format('delta').load(lookup_location+dataset)
                except:
                    # creating schema for source table and reading data
                    schema = StructType([
                                          StructField('advertising_id', StringType(), True),
                                          StructField('masked_advertising_id', StringType(), True),
                                          StructField('start_date', TimestampType(), True),
                                          StructField('end_date', TimestampType(), True),
                                          StructField('flag', StringType(), True)
                                          ])
                    delta_table = spark.createDataFrame(data=[], schema=schema)
                    delta_table.write.format('delta').mode("overwrite").option("overwriteSchema", "true").save(lookup_location+dataset)
                    delta_table = spark.read.format('delta').load(lookup_location+dataset)

                # Join delta_table and source_table 
                source = source_df[dataset]
                joinDF = source.join(delta_table,(source.advertising_id==delta_table.advertising_id) & \
                                        (delta_table.flag=="Active"),"leftouter") \
                                 .select(source["*"], \
                                        delta_table.advertising_id.alias("delta_advertising_id"), \
                                        delta_table.masked_advertising_id.alias("delta_masked_advertising_id"))
                
                # Filtering the new records only
                filter_table = joinDF.filter(xxhash64(joinDF.advertising_id) 
                                             != xxhash64(joinDF.delta_advertising_id))

                # Merging the key columns
                merge_table = filter_table.withColumn("MERGE_KEY",concat(filter_table.advertising_id))

                # Dummy table to insert null key for columns to be updated
                dummy_table = filter_table.filter("delta_advertising_id is not null").withColumn("MERGE_KEY",lit(None))

                # Combining dummy table and merge table
                scd_table = merge_table.union(dummy_table)
                # read delta table in delta format to run .merge method
                Delta_table = DeltaTable.forPath(spark, lookup_location+dataset)
                Delta_table.alias("delta").merge(
                    source = scd_table.alias("source"),
                    condition = "concat(delta.advertising_id) = source.MERGE_KEY and delta.flag='Active'"
                    ).whenMatchedUpdate(set =
                        { 
                            "flag" : "'Inactive'",
                            "end_date":"current_date"
                        }
                    ).whenNotMatchedInsert(values =
                        {
                        "advertising_id" : "source.advertising_id",
                        "masked_advertising_id" : "source.masked_advertising_id",
                        "flag" : "'Active'",
                        "start_date" : "current_date",
                        "end_date": "'None'"
                        }
                    ).execute()
    

    def run(self):
        # Read datasets
        datasets, source_path, destination_path, file_format, partition_by, staging_path, dfs, dfs_original = self.read_datasets()
        # Apply casting to specified columns
        dfs = self.casting(dfs)
        # Apply masking to specified columns
        dfs = self.masking(dfs)
        # create a source dataframe dictionary for lookup dataset
        source_dfs = self.derive_source_df(dfs, dfs_original, datasets)
        # update the lookup datasets
        self.lookup_dataset(source_dfs, datasets)
        # Upload transformed data into staging zone
        self.write_data(dfs, datasets, staging_path, file_format, partition_by)
        # Stop Spark session
        spark.stop()
        
app_name = "MyApp"
app_config_path = "s3://advait-landingbucket-batch08/config/app_config.json"
app = App(app_name, app_config_path)
app.run()
