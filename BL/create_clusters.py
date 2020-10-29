from pyspark.sql import SparkSession
import BL.columns_names as columns_names
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


class Cluster:

    def generate_clasters(self, df):
        # assemble feathers vector
        vec_assembler = VectorAssembler(
            inputCols=[columns_names.gender,
                       columns_names.education,
                       columns_names.age,
                       columns_names.longitude,
                       columns_names.latitude],
            outputCol="features")

        df = vec_assembler.setHandleInvalid("skip").transform(df)

        # Trains a k-means model.
        kmeans = KMeans().setK(5).setInitMode("k-means||").setSeed(1).setFeaturesCol("features")
        #print(kmeans.explainParams())

        model = kmeans.fit(df)

        # Make predictions
        predictions = model.transform(df)

        return predictions.drop(predictions["features"])


