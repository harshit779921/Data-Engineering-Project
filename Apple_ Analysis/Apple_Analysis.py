# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods just after buying Iphone
    """
    def __init__(self):
        pass

    def runner(self):
        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation Logic
        # Customers who have bought Airpods after buying the iPhone
        firstTransformedDF = AirpodsAfterIphoneTransformer(). transform(inputDFs)

        # Step 3: Load all required data to different sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()

# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought  only Iphone 
    """
    def __init__(self):
        pass

    def runner(self):
        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation Logic
        # Customers who have bought Airpods after buying the iPhone
        onlyAirPodsAndIphoneDF = OnlyAirpodsAndIphone(). transform(inputDFs)

        # Step 3: Load all required data to different sink
        OnlyAirPodsAndIphoneLoader(onlyAirPodsAndIphoneDF).sink()

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self,name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFlow().runner() 
        else: 
            raise ValueError(f"Not Implemented for {self.name}")

name = "secondWorkFlow"

workFlowrunner = WorkFlowRunner(name).runner()
