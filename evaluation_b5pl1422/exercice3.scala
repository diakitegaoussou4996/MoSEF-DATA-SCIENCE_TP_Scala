// Databricks notebook source
// MAGIC %md
// MAGIC <br>
// MAGIC <img src="https://i0.wp.com/mosefparis1.fr/wp-content/uploads/2022/10/cropped-image-1.png?fit=532%2C540&ssl=1" width=90px align="left"/>
// MAGIC <div style="text-align: right;">
// MAGIC   <div>Enseignant : Sylla Bachir</div>
// MAGIC   <div>Réalisé par : Gaoussou DIAKITE, Eunice KOFFI et Anisoara ABABII</div>
// MAGIC   <div>Année : 2022/2023</div>
// MAGIC </div>
// MAGIC 
// MAGIC 
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:#010331">Master 2 Modélisation Statistiques Economiques et Financières</span></div><br>
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:28px;color:#e60000">Projet Spark/Scala</span></div><br><br>
// MAGIC 
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>
// MAGIC <br>
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:darkgreen">Exercice 3 : Tracking GPS</span></div><br>
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>

// COMMAND ----------

// DBTITLE 1,Import des modules
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import spark.implicits._
import java.sql.Timestamp

// COMMAND ----------

// DBTITLE 1,Lecture des bases de données
// Lecture des bases de données
val data = spark.read
  .option("header", "true")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/daily_reports/2021-05-07")

// COMMAND ----------

// Vérification
data.show(5)
data.printSchema()

// COMMAND ----------

// DBTITLE 1,Question 1 : Normalisation
def normalizeData(df: DataFrame): DataFrame = {
  val df_normalize =
  df.withColumn("lat", regexp_replace(col("lat"), ",", "."))
    .withColumn("lng", regexp_replace(col("lng"), ",", "."))
    .withColumn("altitude", regexp_replace(col("altitude"), ",", "."))
    .withColumn("power", regexp_replace(col("power"), ",", "."))
    .withColumn("distance", regexp_replace(col("distance"), ",", "."))
    .withColumn("motion", when(col("motion") === 0, false)
                          .when(col("motion") === 1, true)
                          .otherwise(col("motion")))
  return df_normalize
}

// COMMAND ----------

// Vérification
val data_normalize = normalizeData(data)

// COMMAND ----------

// Vérification de la bonne exécution de la méthode
data_normalize.select("motion").show(5)

// COMMAND ----------

// DBTITLE 1,Question 2 : Nettoyage
def cleanData(data: DataFrame, date: String): DataFrame = {
  
  // Filtre les lignes où les colonnes "vin", "dt", "lat" ou "lng" sont null
  val data_cleaned = data.na.drop(Seq("vin", "dt", "lat", "lng"))
  
  // Filtre les lignes où speed > 0 et motion = false
  val data_filtered = data_cleaned.filter(!(col("speed") > 0 && col("motion") === false))
  
  // Sauvegarde les mauvaises lignes
  val defects = 
  data.except(data_filtered)
                    .write
                    .option("header", "true")
                    .mode("overwrite")
                    .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/daily_reports_defects/$date"+".csv")

  // Cast des colonnes dans les types requis
  val cleaned_data = data_filtered
    .withColumn("vin", col("vin").cast("string"))
    .withColumn("dt", col("dt").cast(TimestampType))
    .withColumn("lat", col("lat").cast(DoubleType))
    .withColumn("lng", col("lng").cast(DoubleType))
    .withColumn("altitude", col("altitude").cast(IntegerType))
    .withColumn("speed", col("speed").cast(IntegerType))
    .withColumn("motion", when(col("motion") === 1, false).when(col("motion") === 0, true).otherwise(col("motion")))
    .withColumn("power", col("power").cast(DoubleType))
    .withColumn("distance", col("distance").cast(DoubleType))
    .withColumn("engineHours", col("engineHours").cast(IntegerType))
  
  cleaned_data
}

// COMMAND ----------

// Vérification
val cleaned_data= cleanData(data_normalize, "2012-12-23")

// Impression
cleaned_data.printSchema()

// COMMAND ----------

// Vérification des résultats
spark.read.option("header", "True").csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/daily_reports_defects/2012-12-23.csv").show(5)

// COMMAND ----------

// DBTITLE 1,Question 3 : Enrichissement
def computeAcceleration(df: DataFrame): DataFrame = {
 // Création de la fenêtre pour le calcul de la différence de temps entre les instants successifs
  val timeDiffWindow = Window.partitionBy("vin").orderBy("dt").rowsBetween(Window.currentRow - 1, Window.currentRow)
 // Calcul de la différence de temps entre les instants successifs
  val timeDiff = unix_timestamp(col("dt")).minus(first(unix_timestamp(col("dt"))).over(timeDiffWindow))
 // Calcul de la différence de vitesse entre les instants successifs
  val speedDiff = col("speed").minus(first(col("speed")).over(timeDiffWindow))
 // Calcul de l'accélération moyenne entre les instants successifs
  val acceleration = when(timeDiff.isNotNull && speedDiff.isNotNull && timeDiff.notEqual(0), (speedDiff * 0.44704).divide(timeDiff)).otherwise(null)
 // Ajout de la colonne "acceleration" au DataFrame
  return df.withColumn("acceleration", acceleration)
}

// COMMAND ----------

// Vérification
val data_enrich = computeAcceleration(cleaned_data)

data_enrich.show(5)

// COMMAND ----------

// DBTITLE 1,Question 4 
def computeDrivingHarshness(df: DataFrame): DataFrame = {
 df.withColumn("accelerationHarshness", when('acceleration.isNotNull, 'acceleration - 4).otherwise(null))
   .withColumn("breakingHarshness", when('acceleration.isNotNull, lit(-1) * ('acceleration + 6)).otherwise(null))
}


// COMMAND ----------

// Vérification
val data_acel = computeDrivingHarshness(data_enrich)
computeDrivingHarshness(data_enrich).show(5)


// COMMAND ----------

// DBTITLE 1,Question 5
def resolvePOIs(df: DataFrame): DataFrame = {
  
 // Définition de la fonction de calcul de la distance
val distance = (lat1: Double, lat2: Double, lon1: Double, lon2: Double, alt1: Double, alt2: Double) => {
  val R = 6371 // Radius of the earth
  val latDistance = Math.toRadians(lat2 - lat1)
  val lonDistance = Math.toRadians(lon2 - lon1)
  val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
  val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  var distance = R * c * 1000 // convert to meters
  val height = alt1 - alt2
  distance = Math.pow(distance, 2) + Math.pow(height, 2)
  Math.sqrt(distance)
}
  
 // Lecture de la base pois
val pois = spark.read
  .option("header", "true")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/pois.csv")

// Renommer les colonnes 
val pois_renamed = pois
  .withColumnRenamed("lat", "lat_pois")
  .withColumnRenamed("lng", "lng_pois")
  .withColumnRenamed("altitude", "alt_pois")
  
// Calcul des distances
val distanceUDF = udf(distance)

val poisWithIndexDF = pois_renamed.withColumn("id", monotonically_increasing_id())

val data2WithPoisDF = df.crossJoin(poisWithIndexDF)
  .withColumn("dist", distanceUDF($"lat", $"lat_pois", $"lng", $"lng_pois", $"altitude", $"alt_pois"))

// Groupement des données par vin, dt, lat, lng et altitude, puis calcul du min de dist pour chaque groupe
val minDistDF = data2WithPoisDF.groupBy("vin", "dt", "lat", "lng", "altitude")
                  .agg(min("dist").alias("min_dist"))

// Jointure des données d'origine avec les résultats de minDistDF pour obtenir les lignes correspondantes au min de dist
val resultDF = data2WithPoisDF.join(minDistDF, Seq("vin", "dt", "lat", "lng", "altitude"))
                 .orderBy("vin", "dt").filter($"dist" === $"min_dist")

val resultDFWithPoi = resultDF.withColumn("poi",
  when(col("dist") <= col("radius") * 1000, col("name"))
    .otherwise(lit("null"))
)
.select("vin", "dt", "lat", "lng", "altitude", "speed", "motion", "power", "distance", "engineHours", "acceleration", "accelerationHarshness", "breakingHarshness", "poi")
  
  //

return resultDFWithPoi
  
}


// COMMAND ----------

// Vérification
resolvePOIs(data_acel).show(5)

// COMMAND ----------

// DBTITLE 1,Question 6
def loadGPSData(day: String): DataFrame = {
  val path = s"/FileStore/shared_uploads/evaluation_b5pl1422/tracking/daily_reports/$day/"
spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .csv(path)
}

// COMMAND ----------

// Vérification 
val df = loadGPSData("2021-05-07")
val dfverif = df.groupBy("vin").count()
dfverif.show()

// COMMAND ----------

val df = resolvePOIs(
  computeDrivingHarshness(
    computeAcceleration(
     cleanData(
        normalizeData(
          loadGPSData("2021-05-07")
        ), "2021-05-07"
      )
    )
  )
)

// COMMAND ----------

// Enregistrement de la table dans au format csv
df.write
  .option("header", "true")
  .mode("overwrite")
  .parquet("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/enriched_reports/2021-05-07.parquet")

// COMMAND ----------

// DBTITLE 1,Question 8
// Lecture des bases de données

val table = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/assignments/2021_05_07.csv")

val drivers = spark.read
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/drivers.csv")


// COMMAND ----------

table.show()
table.printSchema(5)

drivers.show()
drivers.printSchema(5)

// COMMAND ----------

// Jointure des bases de données
val drivers_info = drivers.join(table, drivers("id")===table("driverId")).drop("driverId")
val table_complete = df.join(drivers_info, df("vin")===drivers_info("vehicle"))

//Vérification
table_complete.show(5)

// COMMAND ----------

// Calculer les KPIs par conducteur et par heure
val kpis = table_complete
  //Calcul de la colonne heure
  .withColumn("hour", date_trunc("hour", col("dt")))
  .withColumn("accelerationHarshness_bool", when(col("accelerationHarshness") > 0, 1).otherwise(0))
  .withColumn("breakingHarshness_bool", when(col("breakingHarshness") > 0, 1).otherwise(0))
  //Agrégation de la table 
  .groupBy($"hour", $"name")
  // Calcul des indicateurs
  .agg(
    round(sum($"distance"),2).alias("distance"),
    round(sum($"distance"* 0.00039),3).alias("fuelConsumption"),
    sum($"accelerationHarshness_bool").alias("harshAccelerationsCount"), 
    sum($"breakingHarshness_bool").alias("harshBreakingsCount"),
    round(max($"acceleration"),3).alias("maxAcceleration"),
    round(min($"breakingHarshness"),3).alias("maxBreaking"),
    (countDistinct($"poi") -1).alias("visitedPOIsCount")
  )
  .orderBy("hour")

// COMMAND ----------

+-------------------+---------------+--------+---------------+-----------------------+-------------------+---------------+-----------+----------------+
|               hour|     driverName|distance|fuelConsumption|harshAccelerationsCount|harshBreakingsCount|maxAcceleration|maxBreaking|visitedPOIsCount|
+-------------------+---------------+--------+---------------+-----------------------+-------------------+---------------+-----------+----------------+
|2021-05-07 00:00:00|Olivier Gautier| 9147.07|          3.567|                     60|                105|         61.111|    -33.333|               0|
|2021-05-07 00:00:00|     Éric Vidal|  8552.3|          3.335|                      4|                135|          6.944|    -38.889|               1|
|2021-05-07 01:00:00|     Éric Vidal| 30508.4|         11.898|                     19|                327|        166.667|    -55.556|               0|
|2021-05-07 01:00:00|Olivier Gautier|32159.31|         12.542|                    146|                221|        141.667|    -69.444|               0|

// COMMAND ----------

kpis.show(5)

// COMMAND ----------

// Enregistrement de la table dans au format csv
kpis.write
  .option("header", "true")
  .mode("overwrite")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/tracking/kpis/drivers_2021-05-07.csv")

// COMMAND ----------

// DBTITLE 1,Question 9
// MAGIC %python
// MAGIC from pyspark.sql.functions import from_json, col, to_json, struct
// MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
// MAGIC from pyspark.sql import SparkSession
// MAGIC # Création de la session Spark
// MAGIC spark = SparkSession.builder.appName("StreamingApp").getOrCreate()
// MAGIC # Définition du schéma pour les messages de suivi
// MAGIC trackingSchema = StructType([
// MAGIC     StructField("vin", StringType()),
// MAGIC     StructField("timestamp", TimestampType()),
// MAGIC     StructField("latitude", DoubleType()),
// MAGIC     StructField("longitude", DoubleType()),
// MAGIC     StructField("speed", DoubleType()),
// MAGIC     StructField("fuel_level", DoubleType())
// MAGIC ])
// MAGIC # Définition du schéma pour les messages de résolution des points remarquables
// MAGIC poiSchema = StructType([
// MAGIC     StructField("vin", StringType()),
// MAGIC     StructField("dt", TimestampType()),
// MAGIC     StructField("poi", StringType())
// MAGIC ])
// MAGIC # Lecture des messages de suivi à partir du topic Kafka
// MAGIC trackingMessages = spark.readStream.format("kafka") \
// MAGIC     .option("kafka.bootstrap.servers", "localhost:9092") \
// MAGIC     .option("subscribe", "fleet.tracking.telemetry") \
// MAGIC     .load()
// MAGIC # Conversion des données JSON en DataFrame
// MAGIC trackingDF = trackingMessages.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
// MAGIC     .withColumn("tracking", from_json(col("value"), trackingSchema)) \
// MAGIC     .selectExpr("tracking.*")
// MAGIC # Définition du schéma pour les données de suivi enrichies
// MAGIC enrichedSchema = StructType([
// MAGIC     StructField("vin", StringType()),
// MAGIC     StructField("driverId", StringType()),
// MAGIC     StructField("timestamp", TimestampType()),
// MAGIC     StructField("latitude", DoubleType()),
// MAGIC     StructField("longitude", DoubleType()),
// MAGIC     StructField("speed", DoubleType()),
// MAGIC     StructField("fuel_level", DoubleType()),
// MAGIC     StructField("distance", DoubleType()),
// MAGIC     StructField("fuel_consumption", DoubleType()),
// MAGIC     StructField("harsh_accelerations_count", IntegerType()),
// MAGIC     StructField("harsh_brakings_count", IntegerType()),
// MAGIC     StructField("max_acceleration", DoubleType()),
// MAGIC     StructField("max_braking", DoubleType())
// MAGIC ])
// MAGIC # Application des transformations sur les données de suivi
// MAGIC enrichedDF = resolvePOIs(
// MAGIC     computeDrivingHarshness(
// MAGIC         computeAcceleration(
// MAGIC             trackingDF
// MAGIC         )
// MAGIC     )
// MAGIC )
// MAGIC # Écriture des données de suivi enrichies dans le datalake
// MAGIC enrichedDF.writeStream \
// MAGIC     .format("parquet") \
// MAGIC     .option("path", "tracking/enriched_reports_streaming/") \
// MAGIC     .option("checkpointLocation", "tracking/checkpoints/enriched_reports_streaming/") \
// MAGIC     .outputMode("append") \
// MAGIC     .start()
// MAGIC # Lecture des messages de résolution des points remarquables à partir du topic Kafka
// MAGIC poiMessages = spark.readStream.format("kafka") \
// MAGIC     .option("kafka.bootstrap.servers", "localhost:9092") \
// MAGIC     .option("subscribe", "fleet.tracking.pois") \
// MAGIC     .load()
// MAGIC # Conversion des données JSON en DataFrame
// MAGIC poiDF = poiMessages.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
// MAGIC     .withColumn
// MAGIC 
// MAGIC // Enregistrer les données résolues de POI dans un topic Kafka
// MAGIC val poisResolvedStream = poisResolved
// MAGIC   .writeStream
// MAGIC   .format("kafka")
// MAGIC   .option("kafka.bootstrap.servers", "localhost:9092")
// MAGIC   .option("topic", "fleet.tracking.pois")
// MAGIC   .option("checkpointLocation", "/tmp/checkpoints/pois-resolved")
// MAGIC   .start()
// MAGIC // Définir les transformations de la pipeline
// MAGIC val cleanedData = cleanData(gpsStream, "2021-03-05")
// MAGIC val accelerationComputed = computeAcceleration(cleanedData)
// MAGIC val drivingHarshnessComputed = computeDrivingHarshness(accelerationComputed)
// MAGIC val poisResolvedStreamed = resolvePOIs(drivingHarshnessComputed, poisStream)
// MAGIC // Enregistrer les données enrichies dans le datalake
// MAGIC val enrichedData = poisResolvedStreamed
// MAGIC   .writeStream
// MAGIC   .format("parquet")
// MAGIC   .option("path", "tracking/enriched_reports")
// MAGIC   .option("checkpointLocation", "/tmp/checkpoints/enriched-reports")
// MAGIC   .start()
// MAGIC // Lancer la pipeline
// MAGIC //val query = enrichedData
// MAGIC   //.awaitTermination()