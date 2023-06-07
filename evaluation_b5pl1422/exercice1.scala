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
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:#010331">Master 2 Modélistaion Statistiques Economiques et Financières</span></div><br>
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:28px;color:#e60000">Projet Spark/Scala</span></div><br><br>
// MAGIC 
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>
// MAGIC <br>
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:darkgreen">Exercice 1 : Vols USA-Monde</span></div><br>
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>

// COMMAND ----------

// DBTITLE 1,Import des modules 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

// COMMAND ----------

// DBTITLE 1,Question 1
// Définition de la fonction
def loadTrafficData(filePath: String, year: Int): DataFrame = {
  val table = spark.read.format("json").load(filePath)
                   .withColumn("COUNT", col("count").cast("long"))
                   .withColumn("YEAR", lit(year))
  
  return table
}

// Exécution de la fonction pour l'année 2012
val trafficData = loadTrafficData(
  filePath = "/FileStore/shared_uploads/evaluation_b5pl1422/flights/summaries/2012.json",
  year = 2012
)

// COMMAND ----------

// Vérification des résultats
trafficData.show(5)
trafficData.printSchema()

// COMMAND ----------

// DBTITLE 1,Question 2
// Définition de la fonction
def loadTrafficData(year: Int): DataFrame = {
  val filepath = "/FileStore/shared_uploads/evaluation_b5pl1422/flights/summaries/" + year + ".json"
  val table    = spark.read.format("json").load(filepath)
                   .withColumn("COUNT", col("count").cast("long"))
                   .withColumn("YEAR", lit(year))
  
  return table
}

// Exécution de la fonction pour l'année 2012
val trafficData_ = loadTrafficData(year = 2012)

// COMMAND ----------

// Vérification des résultats 
trafficData_.show(5)
trafficData_.printSchema()
trafficData_.count()

// COMMAND ----------

// DBTITLE 1,Question 3
// Importation des tables 

val trafficData_2010 = loadTrafficData(year = 2010)
val trafficData_2011 = loadTrafficData(year = 2011)
val trafficData_2012 = loadTrafficData(year = 2012)
val trafficData_2013 = loadTrafficData(year = 2013)
val trafficData_2014 = loadTrafficData(year = 2014)
val trafficData_2015 = loadTrafficData(year = 2015)

// Concaténation des tables 
val trafficData = trafficData_2010.union(trafficData_2011)
                                  .union(trafficData_2012)
                                  .union(trafficData_2013)
                                  .union(trafficData_2014)
                                  .union(trafficData_2015)

// COMMAND ----------

// Vérification de la bonne exécution du programme
// Compter le nombre d'occurrences de chaque année dans la colonne 'year'
val yearCounts = trafficData.groupBy("year").count()
yearCounts.show()

// COMMAND ----------

// DBTITLE 1,Question 4
trafficData.filter('ORIGIN_COUNTRY_NAME === "United States" && 'DEST_COUNTRY_NAME === "United States").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Nous avons des vols qui circulent à l'intérieur des Etats-Unis. Pour cette question, il conviendra de ne pas prendre en compte ces vols.

// COMMAND ----------

// Filtre sur la table et contruction de deux nouvelles tables

val trafficData_dest   = trafficData.filter('ORIGIN_COUNTRY_NAME==="United States" && 'DEST_COUNTRY_NAME =!= "United States") // Vols entrants des Etats Unis
val trafficData_origin = trafficData.filter('DEST_COUNTRY_NAME==="United States" && 'ORIGIN_COUNTRY_NAME =!= "United States")   // Vols sortants des Etats Unis

val volsEntants   = trafficData_dest.select(sum($"COUNT")).collect()(0)(0).toString.toDouble
val volsSortants  = trafficData_origin.select(sum($"COUNT")).collect()(0)(0).toString.toDouble

// Calcul de la fréquence et ajout de la colonne COUNTRY_NAME
val trafficData_destin  = trafficData_dest.groupBy('DEST_COUNTRY_NAME).agg((sum('COUNT)/volsEntants*100).as("DEST_FREQUENCY"))
                                    .withColumn("COUNTRY_NAME", col("DEST_COUNTRY_NAME").cast("string"))

val trafficData_origine = trafficData_origin.groupBy('ORIGIN_COUNTRY_NAME).agg((sum('COUNT)/volsSortants*100).as("ORIGIN_FREQUENCY"))
                                          .withColumn("COUNTRY_NAME", col("ORIGIN_COUNTRY_NAME").cast("string"))

// Jointure des tables

val traffic = 
trafficData_destin.join(trafficData_origine, "COUNTRY_NAME")
                  .withColumn("OUTWARDNESS", expr("DEST_FREQUENCY - ORIGIN_FREQUENCY"))
                  .select('COUNTRY_NAME, 'DEST_FREQUENCY, 'ORIGIN_FREQUENCY, 'OUTWARDNESS)
                  .orderBy($"DEST_FREQUENCY".desc)
                  

// Enregistrement de la table dans au format csv
traffic.write
  .option("header", "true")
  .mode("overwrite")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/flights/indicators/flights_frequencies.csv")

// COMMAND ----------

// Vérification des résultats
spark.read.option("header", "True").csv("/FileStore/shared_uploads/evaluation_b5pl1422/flights/indicators/flights_frequencies.csv").show(5)

// COMMAND ----------

// DBTITLE 1,Question 5
val trafficData_dest_spec   = trafficData.filter('ORIGIN_COUNTRY_NAME==="United States" && 'DEST_COUNTRY_NAME.isin("Mexico", "Brazil", "United Kingdom", "Russia", "China", "South Africa"))

val table_pivot = trafficData_dest_spec.groupBy('YEAR).pivot("DEST_COUNTRY_NAME").agg(sum('COUNT).as("COUNT_YEAR_COUNTRY")).orderBy("YEAR").select("YEAR", "Mexico", "Brazil", "United Kingdom", "Russia", "China", "South Africa")

// COMMAND ----------

table_pivot.show(5)

// COMMAND ----------

// Enregistrement en mode overwrite

table_pivot.write
  .option("header", "true")
  .mode("overwrite")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/flights/indicators/key_zones_flights.csv")

// COMMAND ----------

// Vérification des résultats
spark.read.option("header", "True").csv("/FileStore/shared_uploads/evaluation_b5pl1422/flights/indicators/key_zones_flights.csv").show()