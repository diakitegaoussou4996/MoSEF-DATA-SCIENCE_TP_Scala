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
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:darkgreen">Exercice 2 : Lutte contre le blanchiment</span></div><br>
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>

// COMMAND ----------

// DBTITLE 1,Import des modules
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spark.implicits._
import org.apache.spark.sql.functions.levenshtein

// COMMAND ----------

// DBTITLE 1,Question 1
// Lecture des bases de données
val signalisations = spark.read
  .option("header", "true")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/fbb/signalisations.csv")

signalisations.show(5)
signalisations.printSchema()

val referentiels = spark.read
  .option("header", "true")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/fbb/referentiel_comptes_clients.csv")
  .withColumn("numero_telephone", concat(lit("+33"), substring('numero_telephone.cast("string"), 3, 10)))

referentiels.show(5)
referentiels.printSchema()

// COMMAND ----------

// DBTITLE 1,Question 2
// Jointure des tables
val table_jointe = signalisations.join(referentiels, signalisations("date_naissance_emetteur") === referentiels("date_naissance"))
                                 .withColumn("nom_complet_emetteur", concat('prenoms_emetteur, lit(" "), 'nom_emetteur))
                                 .withColumn("nom_complet_client", concat('prenoms, lit(" "), 'nom))
                                 .withColumn("distance", levenshtein('nom_complet_client, 'nom_complet_emetteur))
                                 .withColumn("longueur", greatest(length('nom_complet_client), length('nom_complet_emetteur)).cast("int"))
                                 .withColumn("score", lit(1) - ('distance/'longueur))
                                 .filter(col("score") >= 0.7)
table_jointe.select('nom_complet_emetteur, 'nom_complet_client, 'date_naissance_emetteur, 'date_naissance, 'score).show(truncate = false)

// COMMAND ----------

// MAGIC %md 
// MAGIC Nous pouvons voir que le seuil de 0.7 est pertinent parce que ce seuil nous permet de distinguer les individus qui sont les mêmes en ne prenant pas en compte les défaillances de l'algorithme.
// MAGIC 
// MAGIC L'algorithme calcule la distance en se basant uniquement sur l'adéquation absolue entre les deux bases de données. Il ne prend donc pas en compte :
// MAGIC 
// MAGIC   - Les erreurs de saisies 
// MAGIC   - Les abréviations 
// MAGIC 
// MAGIC En effet, nous povons constater que c'est le cas pour les individus qui ont des scores à entre 0.7 et 1. La date de naissance et les noms nous montrent q'il s'agit de la même personne.
// MAGIC 
// MAGIC Par ailleurs, dans le cadre de la recherche de Fraude on devrait baisser le seuil pour avoir plus de vigilance. Nous avons essayé de baisser le seuil à 0.6 et nous avons obtenu les mêmes résultats. 
// MAGIC 
// MAGIC Il conviendra donc à chaque entreprise de définir le seuil qui lui semble pertinent en fonction de son aversion au risque et sa tolérance vis-à-vis de la fraude.

// COMMAND ----------

// DBTITLE 1,Question 3
table_jointe.withColumn("sound_emetteur", soundex('nom_complet_emetteur))
              .withColumn("sound_client", soundex('nom_complet_client))
              .filter(col("score") >= 0.7)
              .select('nom_complet_emetteur, 'nom_complet_client, 'date_naissance_emetteur, 'date_naissance, 'sound_emetteur, 'sound_client, 'score).show(10, truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC On fait un rapprochement basé sur cette fonction et on affiche pour quelques lignes les résultats. 
// MAGIC 
// MAGIC On constate alors que : Soundex reconnait les abréviations, les "noms composés" (exemple : Jean-Pierre). 
// MAGIC 
// MAGIC Cependant, il semble qu'il a du mal avec certains prénoms (Éloïse).

// COMMAND ----------

// DBTITLE 1,Question 4
// MAGIC %md
// MAGIC 
// MAGIC Nous proposons la gestion suivante: 
// MAGIC - implémenter l'algorithme levenshtein sur nom, prénoms et date de naissance puis calculer le score
// MAGIC - implémenter l'algorithme soundex sur le nom, prénoms 
// MAGIC - Choisir un seuil en fonction de notre vision de surveillance
// MAGIC 
// MAGIC 
// MAGIC La fusion de ces deux algorithmes permettra de pallier aux faiblesses les uns des autres.