import os

def clean_column_name(sdf):
    '''
    lower and replace space and dash in pyspark dataframe columns
    '''
    return sdf.toDF(*[c.lower().replace(' ', '_').replace('-','_') for c in sdf.columns])

def upsert_table(spark, updatesDF, condition, output_file, partition_columns=None):
    
    '''
    update/insert transformed immigration data to fact_table
    incase of duplicate id, overwrite the old value with new value, otherwise append it to dataframe
    '''
    
    from delta.tables import DeltaTable

    if not os.path.exists(output_file):
        
        if partition_columns is None:
            updatesDF.write.format('delta').save(output_file)
        else:
            updatesDF.write.format('delta').partitionBy(*partition_columns).save(output_file)
            
    else:
        
        deltaTable = DeltaTable.forPath(spark, output_file)

        deltaTable.alias("source").merge(
            source = updatesDF.alias("update"),
            condition = condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()