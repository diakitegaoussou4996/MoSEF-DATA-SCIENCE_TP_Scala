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
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:darkgreen">Exercice 5 : Supervision à partir de logs</span></div><br>
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC Dans cet exercice, nous allons générer des bases de données factices pour appliquer les différentes méthodes nécessaires à sa réalisation.  
// MAGIC 
// MAGIC C'est pourquoi, nous nous gardons de commenter les résultats du code étant donné qu'ils seront différents à chaque exécution des cellules de génération des tables car les tables générées ne sont pas sauvegardées en mémoire.

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

// MAGIC %md 
// MAGIC 
// MAGIC <div><strong>Quesion 1<div/>
// MAGIC   
// MAGIC Pour cette question, nous allons générer une base de données qui nous servira égalament pour les question 2 et 3.

// COMMAND ----------

// DBTITLE 1,Générer la base de données
import java.io._
import scala.util.Random
import java.time.{Instant, LocalDateTime, ZoneId}

// Génère un UUID aléatoire
def generateUUID(): String = java.util.UUID.randomUUID().toString

// Génère un niveau de log aléatoire
def generateLogLevel(): String = Random.shuffle(Seq("DEBUG", "INFO", "WARNING", "ERROR")).head

// Génère un message de log aléatoire
def generateLogMessage(): String = Random.shuffle(Seq("doing great stuff here", "processing request", "an error occurred", "task completed successfully")).head

// Génère une ligne de log aléatoire pour le composant donné à l'instant donné
def generateLogLine(component: String): String = {
  val correlationId = generateUUID()
  val logLevel = generateLogLevel()
  val logMessage = generateLogMessage()
  val timestamp = LocalDateTime.ofInstant(Instant.now().minusSeconds(Random.nextInt(24 * 60 * 60)), ZoneId.of("UTC"))
  val secondTimestamp = timestamp.plusSeconds(Random.nextInt(24 * 60 * 60))
  s"$timestamp|$secondTimestamp - $component $logLevel - $correlationId - $logMessage"
}

// Génère toutes les lignes de log pour la journée donnée pour chaque composant
def generateLogLinesForDay(day: java.time.LocalDate): Seq[String] = {
  val components = Seq("A", "B", "C")
  for {
    component <- components
  } yield generateLogLine(component)
}

// Génère toutes les lignes de log pour les cinq jours de janvier et les combine en une seule séquence
val allLogLines = (1 to 31).map(day => generateLogLinesForDay(java.time.LocalDate.of(2020, 1, day))).flatten

// Enregistre les lignes de log dans un fichier
val writer = new PrintWriter(new File("logs.txt"))
allLogLines.foreach(line => writer.write(line + "\n"))
writer.close()

// Crée un DataFrame à partir de la séquence combinée de lignes de log
val rdd = spark.sparkContext.parallelize(allLogLines)
import spark.implicits._
val df = rdd.toDF("log")

// Vérification de la table 
df.show(truncate=false)
df.count()

// COMMAND ----------

// DBTITLE 1,Extraction des colonnes
// Extraire la date (jour)
val day = regexp_extract($"log", "^(\\d{4}-\\d{2}-\\d{2})", 1).alias("day")
// Extraire la composante
val composante = regexp_extract($"log", "\\s-\\s([A-Z])\\s", 1).alias("composante")
// Extraire le niveau de log
val niveau_log = regexp_extract($"log", "\\s-\\s[A-Z]{1}\\s([A-Z]+)\\s-", 1).alias("niveau_log")
// Extraire l'ID de corrélation
val correlation_id = regexp_extract($"log", "-\\s([\\d\\w-]+)\\s-", 1).alias("correlation_id")
// Extraire le message
val message = regexp_extract($"log", "\\s-\\s([\\w\\s]+)$", 1).alias("message")

val date_start = regexp_extract($"log", "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})", 1).alias("date_start")

val date_end = regexp_extract($"log", "\\|(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})", 1).alias("date_end")


val result = df.select(day, date_start, date_end, composante, niveau_log, correlation_id, message)
                  .withColumn("date_start", col("date_start").cast("timestamp"))
                  .withColumn("date_end", col("date_end").cast("timestamp"))
                  .orderBy(col("date_start"))

result.show(false)

// COMMAND ----------

// Vérification
result.printSchema()
result.groupBy("day").count().show()
result.groupBy("composante").count().show()
result.groupBy("niveau_log").count().show()

// COMMAND ----------

// DBTITLE 0,Question 1
import org.apache.spark.sql.functions.{col, date_trunc, count}
import org.apache.spark.sql.types.TimestampType


// Group by day and period_start (in 30-minute intervals)
val df = result
  .groupBy(
    col("day"),
    date_trunc("hour", col("date_start")).as("period_start"),
    ((col("date_start").cast("long") - date_trunc("hour", col("date_start")).cast("long")) / (30 * 60)).cast("int").as("period")
  )
  .agg(count("*").as("processed_requests_count"))

// Add period_end column by adding 30 minutes to period_start
val resultWithPeriodEnd = df.withColumn("period_end", col("period_start") + expr("INTERVAL 30 MINUTES"))

// Select the required columns and order by day and period_start
val finalResult = resultWithPeriodEnd.select(col("day"), col("period_start"), col("period_end"), col("processed_requests_count"))
  .orderBy(col("day"), col("period_start"))

// Display the result
finalResult.show()


// COMMAND ----------

// MAGIC %md 
// MAGIC <div><Strong>Question 2</div>

// COMMAND ----------

import org.apache.spark.sql.functions.{col, date_trunc, count, sum, expr}

// Regrouper les données par jour et heure, et compter le nombre de demandes traitées dans chaque période.
val activityDF = result.groupBy(date_trunc("day", col("date_start")).alias("day"), 
                            date_trunc("hour", col("date_start")).alias("hour"))
                   .agg(sum(expr("CASE WHEN date_start IS NOT NULL THEN 1 ELSE 0 END")).alias("processed_requests_count"))

activityDF.show(3)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, date_add}

// Filtrer les périodes d'activité
val activePeriodsDF = activityDF
  .filter(col("processed_requests_count") > 0)
  .withColumn("period_start", col("hour"))
  .withColumn("period_end", date_add(col("period_start"), 1))
  
activePeriodsDF.show(3)

// COMMAND ----------

// Définir la fonction quart d'heure
def quarterhour(col: Column) = (floor(minute(col) / 15) * 15).cast("int")

// Convertir les colonnes period_start et period_end en type horodatage
val df = finalResult.select(
  col("day"),
  unix_timestamp(col("period_start")).cast("timestamp").alias("period_start"),
  unix_timestamp(col("period_end")).cast("timestamp").alias("period_end"),
  col("processed_requests_count")
)

// Agréger les requêtes par jour et intervalles de quart d'heure
val df_agg = df.select(
  date_trunc("day", col("period_start")).alias("day"),
  quarterhour(col("period_start")).alias("interval_start"),
  quarterhour(col("period_end")).alias("interval_end"),
  col("processed_requests_count")
).groupBy("day", "interval_start", "interval_end").agg(
  sum("processed_requests_count").alias("requests_count")
)

// Définir une fonction de fenêtre pour calculer l'heure de fin de l'intervalle précédent.
val w = Window.partitionBy("day").orderBy("interval_start")
val df_lag = df_agg.select(
  col("*"),
  lag("interval_end", 1, 0).over(w).alias("prev_interval_end")
)

df.show(2)
df_agg.show(2)
df_lag.show(2)

// COMMAND ----------

// MAGIC %md
// MAGIC Sur toutes les périodes il y a été exércé au moins une activité, on se retrouve à la fin avec une table df_inactive vide

// COMMAND ----------

// Calculer les périodes d'inactivité du système
val df_inactive = df_lag.filter(
  (coalesce(col("prev_interval_end"), lit(0)) < col("interval_start")) && (col("requests_count") === 0)
).select(
  col("day"),
  col("prev_interval_end").alias("inactive_start"),
  col("interval_start").alias("inactive_end")
)

// Vérification
df_inactive.show(3)

// COMMAND ----------

// MAGIC %md 
// MAGIC <div><Strong>Question 3</div>

// COMMAND ----------

// DBTITLE 0,Question 5.3
// Créer une nouvelle colonne pour indiquer si une demande est incomplète
val incompleteDf = result
  .filter(col("composante") === "B")
  .withColumn("is_incomplete", col("date_end").isNull)

// Regrouper par jour et intervalles de 30 minutes, et compter le nombre de demandes incomplètes
val incompleteCountDf = incompleteDf
  .groupBy(col("day"), window(col("date_start"), "30 minutes"))
  .agg(sum(when(col("is_incomplete"), 1).otherwise(0)).alias("incomplete_requests_count"))

// Vérification
incompleteCountDf.show(truncate=false)


// COMMAND ----------

// MAGIC %md 
// MAGIC <div><Strong>Question 4</div>
// MAGIC   
// MAGIC Pour cette question nous allons générer une nouvelle base de données.

// COMMAND ----------

// DBTITLE 0,Question 5.4 (New Dataframe created for this exercice)
// Définir la liste des noms de composants
val componentNames = Seq("A", "B", "C")

// Générer des données de métriques fictives pour un composant donné et une plage de temps
def generateMetrics(component: String, start: Timestamp, end: Timestamp): Seq[(Timestamp, String, Int, Int)] = {
  val random = new Random()
  val durationMs = end.getTime - start.getTime
  val timestamps = Seq.iterate(start, (durationMs / 60000).toInt + 1)(t => new Timestamp(t.getTime + 60000))
  timestamps.map { timestamp =>
    val ramUsage = random.nextInt(80) + 10
    val cpuUsage = random.nextInt(60) + 20
    (timestamp, component, ramUsage, cpuUsage)
  }
}

// Générer des données de métriques fictives pour tous les composants et une plage de temps donnée
def generateAllMetrics(start: Timestamp, end: Timestamp): Seq[(Timestamp, String, Int, Int)] = {
  componentNames.flatMap(component => generateMetrics(component, start, end))
}

// Générer des données de métriques fictives pour une période d'un jour
val startDate = Timestamp.valueOf("2023-03-10 00:00:00")
val endDate = Timestamp.valueOf("2023-03-11 00:00:00")
val metricsData = generateAllMetrics(startDate, endDate)

// Convertir les données de métriques en un DataFrame
val metricsSchema = StructType(Seq(
  StructField("timestamp", TimestampType, nullable = false),
  StructField("component", StringType, nullable = false),
  StructField("ram_usage", IntegerType, nullable = false),
  StructField("cpu_usage", IntegerType, nullable = false)
))
val metrics = metricsData.toDF("timestamp", "component", "ram_usage", "cpu_usage").withColumn("requests_count", lit(1)).select("timestamp", "component", "requests_count", "ram_usage", "cpu_usage")

// Vérification
metrics.show()


// COMMAND ----------

//Vérification
metrics.groupBy("component").count().show()

// COMMAND ----------

// Définir le schéma des données de métriques
val metricsSchema = StructType(Seq(
  StructField("timestamp", TimestampType, nullable = false),
  StructField("component", StringType, nullable = false),
  StructField("ram_usage", IntegerType, nullable = false),
  StructField("cpu_usage", IntegerType, nullable = false)
))

// Regrouper les métriques par composant et fenêtre temporelle
val groupedMetrics = metrics.groupBy(
  window($"timestamp", "1 minute"), $"component"
).agg(
  count("*").alias("requests_count"),
  max($"ram_usage").alias("max_used_ram"),
  max($"cpu_usage").alias("max_used_cpu"),
  avg($"ram_usage").alias("avg_used_ram"),
  avg($"cpu_usage").alias("avg_used_cpu"),
  expr("percentile_approx(ram_usage, 0.7)").alias("percentile_70_used_ram"),
  expr("percentile_approx(cpu_usage, 0.7)").alias("percentile_70_used_cpu")
)

// Aplatir la colonne fenêtre et renommer les colonnes
val dataset = groupedMetrics.select(
  $"window.start".alias("datetime_timestamp"),
  $"component",
  $"requests_count",
  $"max_used_ram",
  $"max_used_cpu",
  $"avg_used_ram",
  $"avg_used_cpu",
  $"percentile_70_used_ram",
  $"percentile_70_used_cpu"
)

// Afficher l'ensemble de données résultant
dataset.show()