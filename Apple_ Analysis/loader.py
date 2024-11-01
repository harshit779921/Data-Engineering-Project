# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDf = transformedDF

    def sik(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDf,
            path = "dbfs:/FileStore/tables/airpods_after_iphone",
            method = "overwrite"
        ).load_data_frame()

class OnlyAirPodsAndIphoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionsByColumn": ["location"]
        }
        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.transformedDf,
            path = "dbfs:/FileStore/tables/airpods_only_iphone",
            method = "overwrite",
            params = params
        ).load_data_frame()

        get_sink_source(
            sink_type = "delta",
            df = self.transformedDf,
            path = "default.onlyAirPodsAndIphone",
            method = "overwrite",
            params = params
        ).load_data_frame()
