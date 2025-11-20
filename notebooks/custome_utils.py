from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, row_number, col, current_timestamp
from pyspark.sql.window import Window
from typing import List
from delta.tables import DeltaTable

class transformations:

    def dedupDF(self, df:DataFrame, dedup_cols:List, key_col:str):
        """ This function will perform deduplication logic based on the df, columns """

        df = df.withColumn("dedup_key", concat(*dedup_cols)) #the * value extract list to a string- unpack string
        df = df.withColumn("dedup_count", row_number().over(Window.partitionBy("dedup_key").orderBy(col(key_col).desc())))
        df = df.drop("dedup_key", "dedup_count")
        return df
    
    def process_timestamp(self, df:DataFrame):
        """ This function adds a new column called process_timestamp to the given dataframe"""

        df = df.withColumn("process_timestamp", current_timestamp())
        return df   
    
    def upsert(self, spark, df:DataFrame, table:str, join_cols:List, time_col:str):
        """ This function perform the upsert operation """

        delta_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")
        merge_condition = " AND ".join([f"src.{col} == tgt.{col}" for col in join_cols])
        delta_obj.alias("tgt").merge(df.alias("src"), condition = merge_condition)\
                                .whenMatchedUpdateAll(f"src.{time_col} >= tgt.{time_col}")\
                                .whenNotMatchedInsertAll()\
                                .execute()
                
        return 1

