# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains

class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Find customers who have bought AirPods after buying the iPhone
        """

        # Fetch the transaction input DataFrame
        transcationInputDF = inputDFs.get("transcationInputDF")

        print("Transaction DataFrame in transform:")
        transcationInputDF.show()

        # Define a window specification partitioned by customer_id and ordered by transaction_date
        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        # Add a new column that shows the next product bought by the customer
        transformedDF = transcationInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("AirPods after buying iPhone (transformed DataFrame):")
        transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

        # Filter customers who bought iPhone followed by AirPods
        filteredDF = transformedDF.filter(
            (col("product_name") == 'iPhone') & (col("next_product_name") == 'AirPods'))

        print("Filtered DF (AirPods after iPhone):")
        filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")
        customerInputDF.show()

        # Perform the join
        joinDF = customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("Joined DF:")
        joinDF.show()

        # Return selected columns
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )


class OnlyAirpodsAndIphone(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought only iPhone and AirPods, nothing else
        """
        transcationInputDF = inputDFs.get("transcationInputDF")

        print("Transaction DataFrame in transform:")
        transcationInputDF.show()

        # Group the DataFrame by customer_id and collect distinct products as a set
        groupedDF = transcationInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped DF (by customer and products):")
        groupedDF.show()

        # Filter customers who bought exactly iPhone and AirPods
        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)  # Only customers with exactly 2 products
        )

        print("Filtered DF (Only iPhone and AirPods):")
        filteredDF.show()

        customerInputDF = inputDFs.get("customerInputDF")
        customerInputDF.show()

        # Perform the join
        joinDF = customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("Joined DF:")
        joinDF.show()

        # Return selected columns
        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

