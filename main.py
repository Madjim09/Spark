from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def process_products_categories(products_df, categories_df):
    joined_df = products_df.join(categories_df, "productId", "left")
    pairs_df = joined_df.select("productName", "categoryName").na.drop()
    no_categories_df = products_df.join(categories_df, "productId", "left_anti").select("productName")
    return pairs_df, no_categories_df


spark = SparkSession.builder \
    .appName("ProductsCategories") \
    .getOrCreate()

products_data = [("product1", 1), ("product2", 2), ("product3", None)]
categories_data = [(1, "category1"), (2, "category2")]

products_df = spark.createDataFrame(products_data, ["productName", "productId"])
categories_df = spark.createDataFrame(categories_data, ["productId", "categoryName"])

pairs_df, no_categories_df = process_products_categories(products_df, categories_df)

print("Пары \"Имя продукта - Имя категории\":")
pairs_df.show()

print("Продукты без категорий:")
no_categories_df.show()

spark.stop()
