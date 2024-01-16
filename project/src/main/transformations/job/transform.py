from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,BooleanType
from pyspark.sql.functions import col, max
class spark_app():
    def __init__(self):
        self.spark = SparkSession.builder.master("local[*]").\
                appName("Transformation").\
                getOrCreate()


    def read_file(self):
        my_schema = StructType([
            StructField("video_id", StringType(), True),  # StringType: You can change to appropriate data type
            StructField("trending_date", TimestampType(), True),
            StructField("title", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("category_id", IntegerType(), True),
            StructField("publish_time", TimestampType(), True),
            StructField("tags", StringType(), True),
            StructField("views", IntegerType(), True),
            StructField("likes", IntegerType(), True),
            StructField("dislikes", IntegerType(), True),
            StructField("comment_count", IntegerType(), True),
            StructField("thumbnail_link", StringType(), True),
            StructField("comments_disabled", BooleanType(), True),
            StructField("ratings_disabled", BooleanType(), True),
            StructField("video_error_or_removed", BooleanType(), True),
            StructField("description", StringType(), True)

        ])

        local_file_path = "C:\\Users\\asus\\Documents\\project_utube_analysis\\datafiles\\CA\\CAvideos.csv"
        df = self.spark.read.csv(local_file_path, header = True, schema = my_schema)
        #df.show()
        #df.filter(max("likes")).show()
        #convert into temp table
        #df.printSchema()
        temp_table_name = "CAvideos_csv"
        df.createOrReplaceTempView(temp_table_name)

        result = self.spark.sql("SELECT * FROM CAvideos_csv WHERE likes IN (SELECT MAX(likes) FROM CAvideos_csv)")
        result.show()

        max_likes_value = df.agg(max(col("likes"))).collect()[0][0]
        print(max_likes_value)
        result_df = df.filter(col("likes") == max_likes_value)
        result_df.show()

    def
if __name__=="__main__":
    obj = spark_app()
    obj.read_file()
