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
// MAGIC <div style="text-align: center;"><span style="font-family:Lucida Caligraphy;font-size:32px;color:darkgreen">Exercice 4 :  Maintenance matériel</span></div><br>
// MAGIC <div align="center" style="border-bottom: 2px solid black;"></div>

// COMMAND ----------

// MAGIC %md
// MAGIC Dans cette partie, nous avons construit une base de données sous excel en suivant certains critères. 
// MAGIC 
// MAGIC La base de données a ensuite été chargée en format csv pour la réalisation de notre travail.

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
val schema1 = StructType.fromDDL("numero_intervention STRING,code_operation STRING,id_materiel STRING,date_debut_intervention TIMESTAMP,date_fin_intervention TIMESTAMP,releve_km Int")
val schema2 = StructType.fromDDL("id_materiel STRING,etat_organe STRING,date_echeance TIMESTAMP")

val maintenance = spark.read
  //.schema(schema1)
  .option("header", "true")
  .option("delimiter", ";")
  //.option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/exercice4/maintenance-2.csv")

val tracabilite = spark.read
  //.schema(schema2)
  .option("header", "true")
  .option("delimiter", ";")
  .option("inferSchema", "true")
  .csv("/FileStore/shared_uploads/evaluation_b5pl1422/exercice4/tracabilite-2.csv")

maintenance.show()
tracabilite.show()

// COMMAND ----------

// Convertir les colonnes date_debut_intervention et date_fin_intervention en timestamp
val maintenance_ts = maintenance.withColumn("date_debut_intervention", unix_timestamp($"date_debut_intervention", "dd/MM/yyyy HH:mm:ss").cast("timestamp"))
    .withColumn("date_fin_intervention", unix_timestamp($"date_fin_intervention", "dd/MM/yyyy HH:mm:ss").cast("timestamp"))

val tracabilite_ts = tracabilite.withColumn("date_echeance", unix_timestamp($"date_echeance", "dd/MM/yyyy HH:mm:ss").cast("timestamp"))
  

maintenance_ts.printSchema()
maintenance_ts.show()

tracabilite_ts.printSchema()
tracabilite_ts.show()

// COMMAND ----------

// DBTITLE 1,Question 1
// Calcul de delta
val delta = maintenance_ts
.select($"id_materiel", $"date_debut_intervention", $"date_fin_intervention", $"code_operation", $"releve_km", lead($"date_debut_intervention", 1)
      .over(Window.partitionBy($"id_materiel").orderBy($"date_debut_intervention")).as("next_date_debut_intervention"))
.withColumn("delta", ($"next_date_debut_intervention" - $"date_debut_intervention") * 0.2)

delta.show(truncate=false)

// Effectuer le rapprochement entre les deux datasets
val joined = delta.alias("m").join(tracabilite_ts.alias("t"), delta("id_materiel")===tracabilite_ts("id_materiel") && (tracabilite_ts("date_echeance") > delta("date_debut_intervention") - delta("delta") && tracabilite_ts("date_echeance") <= delta("next_date_debut_intervention") - delta("delta")), "left").drop(tracabilite_ts("id_materiel"))

joined.show(truncate=false)

// Mettre TM pour les lignes sans rappochements
val joined_filled = joined.na.fill("TM", Seq("etat_organe"))
                          .select('id_materiel, 'etat_organe, 'date_debut_intervention, 'date_fin_intervention, 'releve_km)
                          .orderBy('id_materiel, 'date_debut_intervention)

joined_filled.show(truncate=false)

// COMMAND ----------

// DBTITLE 1,Question 2
// MAGIC %md
// MAGIC Pour cette question nous appliquerons la règle uniquement à un seul id en guise d'exemple

// COMMAND ----------

// Calcul pour un véhicule 

// 1. On filtre les lignes avec id = X75009.15600.4455
val df_id_first = joined_filled.filter(col("id_materiel") === "X75009.15600.4455")
//df_id_first.show()

// 2. On trie la table par date_debut_intervention
val df_id_first_filtered = df_id_first.orderBy("date_debut_intervention")
//df_id_first_filtered.show()

//3. 
val states = df_id_first_filtered.groupBy("id_materiel").agg(collect_list("etat_organe").as("etats_organe"))

val df_with_states = df_id_first_filtered
  .join(states, Seq("id_materiel"), "left_outer")
//df_with_states.show()

// Ajouter une colonne avec la valeur du premier élément de la liste

val statesWithFirstElement = df_with_states.withColumn("first_element", $"etats_organe"(0))
//statesWithFirstElement.show()

val df_rc_position = statesWithFirstElement
  .withColumn("rc_position", array_position(slice($"etats_organe", lit(2), size($"etats_organe")), "RC"))
  .withColumn("next_rc", when($"rc_position" > 0, 1).otherwise(0))
  .drop("rc_position")

val element_position = statesWithFirstElement
  .withColumn("tm_position", array_position(slice($"etats_organe", lit(2), size($"etats_organe")), "TM") + lit(1))
  .withColumn("rc_position", array_position(slice($"etats_organe", lit(2), size($"etats_organe")), "RC") + lit(1))
  .withColumn("nc_position", array_position(slice($"etats_organe", lit(2), size($"etats_organe")), "NC") + lit(1))
  .withColumn("c_position", array_position(slice($"etats_organe", lit(2), size($"etats_organe")), " C"))
  .withColumn("min_position", least($"tm_position", $"rc_position", $"nc_position").cast("int"))
  .withColumn("element_trouve", element_at($"etats_organe", $"min_position"))
  .drop("tm_position", "rc_position", "nc_position", "min_position")

// Vérification
element_position.show()


// COMMAND ----------

// DBTITLE 1,Analyse des conditions de calculs
// MAGIC %md 
// MAGIC Une fois le rapprochement fait, nous pouvons calculer les autres colonnes en se basant sur les règles de calcul suivantes :
// MAGIC 
// MAGIC - Trier la table par id_materiel et date_debut_intervention
// MAGIC - Pour chaque id_materiel on regarde  etat :
// MAGIC     - si first_element = RC :
// MAGIC       - et element_trouve =  RC
// MAGIC       
// MAGIC             Type d'individu : défaillant
// MAGIC             Durée de vie jour : date_debut(RC+1) - date_fin(RC)
// MAGIC             Durée de vie km :releve_km(RC+1) - releve_km(RC)
// MAGIC             Ligne calcul : Ligne 1er RC
// MAGIC                       
// MAGIC       - et element_trouve =  NC :
// MAGIC        
// MAGIC             Type d'individu : Défaillant
// MAGIC             Durée de vie jour : date_debut(NC) - date_fin(RC)
// MAGIC             Durée de vie km : releve_km(NC) - releve_km(RC)
// MAGIC             Ligne calcul : Ligne 1er RC
// MAGIC                       
// MAGIC       - si le prochain n'est ni "NC", "RC", "TM" et c_position >= 1
// MAGIC                 
// MAGIC             Type d'individu : Censuré à droite
// MAGIC             Durée de vie jour : date_debut(dernier C) - date_fin(RC)
// MAGIC             Durée de vie km : releve_km(dernier C) - releve_km(RC)
// MAGIC             Ligne calcul : Ligne RC
// MAGIC                       
// MAGIC       - sinon : pas de calcul  
// MAGIC                 
// MAGIC      - si first_element = NC :
// MAGIC        - et element_trouve = RC et c_position >=1
// MAGIC        
// MAGIC              Type d'individu : Censuré à gauche
// MAGIC              Durée de vie jour : date_debut(RC) - date_fin (1er C)
// MAGIC              Durée de vie km : releve_km(RC) - releve_km(1er C)
// MAGIC              Ligne calcul : Ligne 1er NC
// MAGIC 
// MAGIC        - et element_trouve = NC et  c_position >=1:
// MAGIC         
// MAGIC                
// MAGIC               Type d'individu : Censuré à gauche
// MAGIC               Durée de vie jour : date_debut(NC+1) - date_fin(1er C)
// MAGIC               Durée de vie km : releve_km(NC+1) - releve_km(1er C)
// MAGIC               Ligne calcul : Ligne 1er C
// MAGIC                       
// MAGIC        - et element_trouve = TM et c_position >=1 :
// MAGIC 
// MAGIC               Type d'individu : Censuré à droite
// MAGIC               Durée de vie jour : date_debut(TM) - date_fin(1er C)
// MAGIC               Durée de vie km : releve_km(TM) - releve_km(1er C)
// MAGIC               Ligne calcul : Ligne NC
// MAGIC                       
// MAGIC         - sinon : pas de calcul  
// MAGIC                       
// MAGIC                 
// MAGIC      - si first_element = TM :
// MAGIC        - et element_trouve = RC et c_position >=1
// MAGIC                     
// MAGIC               Type d'individu : Censuré à gauche
// MAGIC               Durée de vie jour : date_debut(RC) - date_fin (1er C)
// MAGIC               Durée de vie km : releve_km(RC) - releve_km(1er C)
// MAGIC               Ligne calcul : Ligne 1er NC
// MAGIC                       
// MAGIC        - et element_trouve = TM et c_position >=1 :
// MAGIC                
// MAGIC               Type d'individu : Censuré
// MAGIC               Durée de vie jour : date_debut(TM+1) - date_fin(1er C)
// MAGIC               Durée de vie km : releve_km(TM+1) - releve_km(1er C)
// MAGIC               Ligne calcul : Ligne 1er C
// MAGIC                       
// MAGIC        - on element_trouve = NC et c_position >=1 :
// MAGIC                 
// MAGIC               Type d'individu : Censuré à gauche
// MAGIC               Durée de vie jour : date_debut(NC) - date_fin(1er C)
// MAGIC               Durée de vie km : releve_km(NC) - releve_km(1er C)
// MAGIC               Ligne calcul : Ligne 1er C
// MAGIC                       
// MAGIC         - sinon : pas de calcul 
// MAGIC                 
// MAGIC      - Sinon Pas de calcul        

// COMMAND ----------

// Définition des conditions

val condition1 = ($"first_element" === "RC") && ($"element_trouve" === "RC")
val condition2 = ($"first_element" === "RC") && ($"element_trouve" === "NC")
val condition3 = ($"first_element" === "RC") && ($"element_trouve".notEqual("NC")) && ($"element_trouve".notEqual("RC")) && ($"element_trouve".notEqual("TM")) && ($"c_position" >= 1)
val condition4 = ($"first_element" === "NC") && ($"element_trouve" === "RC") && ($"c_position" >= 1)
val condition5 = ($"first_element" === "NC") && ($"element_trouve" === "NC") && ($"c_position" >= 1)
val condition6 = ($"first_element" === "NC") && ($"element_trouve" === "TM") && ($"c_position" >= 1)
val condition7 = ($"first_element" === "TM") && ($"element_trouve" === "RC") && ($"c_position" >= 1)
val condition8 = ($"first_element" === "TM") && ($"element_trouve" === "NC") && ($"c_position" >= 1)
val condition9 = ($"first_element" === "TM") && ($"element_trouve" === "TM") && ($"c_position" >= 1)

// COMMAND ----------

// Application à notre table pour exemple

// Elle se trouve dans la condition 1
val resultDF = element_position.withColumn("duree_vie_jour", datediff(lead("date_debut_intervention", 1).over(Window.partitionBy("id_materiel").orderBy("date_debut_intervention")), col("date_fin_intervention")))
                         .withColumn("duree_vie_km", lead("releve_km", 1).over(Window.partitionBy("id_materiel").orderBy("date_debut_intervention")).minus(col("releve_km")))
                         .withColumn("type_individu", lit("defaillant"))
                         .withColumn("duree_de_vie", struct(col("duree_vie_jour"), col("duree_vie_km")))
                         .select("id_materiel", "etat_organe", "date_debut_intervention", "date_fin_intervention", "releve_km", "duree_de_vie", "type_individu")
resultDF.show()
resultDF.printSchema()