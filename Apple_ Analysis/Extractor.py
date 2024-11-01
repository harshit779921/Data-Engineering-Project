# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class
    """
    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transcationInputDF = get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transcationInputDF.orderBy("customer_id","transaction_date").show()

        customerInputDF = get_data_source(
            data_type = "delta",
            file_path = "default.customer_updated_delta"
        ).get_data_frame()

        customerInputDF.show()

        inputDFs = {
            "transcationInputDF": transcationInputDF,
            "customerInputDF": customerInputDF
        }

        return inputDFs


# COMMAND ----------


