// Databricks notebook source
// DBTITLE 1,Olympic Games Dataframe

val athlete_events = sqlContext.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/FileStore/tables/athlete_events-2.csv")

display(athlete_events)

// COMMAND ----------


athlete_events.printSchema

// COMMAND ----------


import spark.implicits._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions;

val athlete_events_final = athlete_events.withColumn("ID",'ID.cast("Double"))
                                         .withColumn("Age",'Age.cast("Double")) 
                                         .withColumn("Height",'Height.cast("Double")) 
                                         .withColumn("Weight",'Weight.cast("Double"))
                                         .withColumn("Year",'Year.cast("Double"))

// COMMAND ----------


athlete_events_final.printSchema

// COMMAND ----------


display(athlete_events_final)

// COMMAND ----------

// DBTITLE 1,Creating Temporary View

athlete_events_final.createOrReplaceTempView("athlete_events_final")

// COMMAND ----------

// DBTITLE 1,NOC Region DF

val noc_regions = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/FileStore/tables/noc_regions.csv")

display(noc_regions)

// COMMAND ----------


noc_regions.printSchema

// COMMAND ----------

// DBTITLE 1,Creating Temporary View from Data Frame

noc_regions.createOrReplaceTempView("noc_regions")

// COMMAND ----------

// MAGIC %sql 
// MAGIC
// MAGIC select * from athlete_events_final;

// COMMAND ----------

// DBTITLE 1,Distribution of the age of gold medalists
// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, Age from athlete_events_final where Medal = 'Gold' group by Age order by Age asc;

// COMMAND ----------

// DBTITLE 1,Gold Medals for Athletes Over 50 based on Sports
// MAGIC %sql
// MAGIC
// MAGIC select Sport, Age from athlete_events_final where Medal = 'Gold' and Age >= 50; 

// COMMAND ----------

// DBTITLE 1,Women medals per edition(Summer Season) of the Games
// MAGIC %sql 
// MAGIC
// MAGIC select count(Medal) as Medals, Year from athlete_events_final where Sex = 'F' and Season = 'Summer' and Medal in ('Bronze','Gold','Silver') group by Year order by Year asc;

// COMMAND ----------

// DBTITLE 1,Top 5 Gold Medal Countries
// MAGIC %sql 
// MAGIC
// MAGIC select count(Medal) as Medals, region from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' group by region order by Medals desc limit 5; 

// COMMAND ----------

// DBTITLE 1,Disciplines with the greatest number of Gold Medals
// MAGIC %sql 
// MAGIC
// MAGIC select count(Medal) as Medals, Event from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' and A.NOC = 'USA' group by Event order by Medals desc; 

// COMMAND ----------

// DBTITLE 1,Height vs Weight of Olympic Medalists
// MAGIC %sql
// MAGIC
// MAGIC select Weight, Height from athlete_events_final where  Medal = 'Gold'; 

// COMMAND ----------

// DBTITLE 1,Variation of Male Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select count(Sex) as Males, Year from athlete_events_final where Sex = 'M' and Season = 'Summer' group by Year order by Year asc; 
// MAGIC

// COMMAND ----------

// DBTITLE 1,Variation of Female Athletes over time
// MAGIC %sql 
// MAGIC
// MAGIC select count(Sex) as Females, Year from athlete_events_final where Sex = 'F' and Season = 'Summer' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Age for Male Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select min(Age),mean(Age), max(Age), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Age for Female Athletes over time
// MAGIC %sql
// MAGIC select min(Age),mean(Age), max(Age), Year from athlete_events_final where Sex = 'F' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Weight for Male Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Weight for Female Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sex = 'F' and Year > 1925 group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Height for Male Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sex = 'M' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Variation of Height for Female Athletes over time
// MAGIC %sql
// MAGIC
// MAGIC select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sex = 'F' group by Year order by Year asc; 

// COMMAND ----------

// DBTITLE 1,Weight over year for Male Gymnasts
// MAGIC %sql
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Gymnastics' and Sex = 'M' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Weight over year for Female Gymnasts
// MAGIC %sql
// MAGIC
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Gymnastics' and Sex = 'F' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Weight over years for Male Lifters
// MAGIC %sql
// MAGIC
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'M' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Weight over year for Female Lifters
// MAGIC %sql
// MAGIC select min(Weight),mean(Weight), max(Weight), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'F' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Height over year for Male Lifters
// MAGIC %sql
// MAGIC
// MAGIC select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'M' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Height over year for Female Lifters
// MAGIC %sql
// MAGIC select min(Height),mean(Height), max(Height), Year from athlete_events_final where Sport = 'Weightlifting' and Sex = 'F' and Year > 1950 group by Year order by Year;

// COMMAND ----------

// DBTITLE 1,Gold Medals based on Countries
// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Gold' group by N.NOC

// COMMAND ----------

// DBTITLE 1,Silver Medals based on Countries
// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Silver' group by N.NOC

// COMMAND ----------

// DBTITLE 1,Bronze Medals based on Countries
// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, N.NOC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC  where Medal = 'Bronze' group by N.NOC

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, case 
// MAGIC 				WHEN N.NOC='ALG' THEN 'DZA'
// MAGIC 				WHEN N.NOC='ANZ' THEN 'AUS'
// MAGIC 				WHEN N.NOC='BAH' THEN 'BHS'
// MAGIC                 WHEN N.NOC='BUL' THEN 'BGR'
// MAGIC 				WHEN N.NOC='CRC' THEN 'CRI'
// MAGIC 				WHEN N.NOC='CRO' THEN 'HRV'
// MAGIC                 WHEN N.NOC='DEN' THEN 'DNK'
// MAGIC 				WHEN N.NOC='EUN' THEN 'RUS'
// MAGIC 				WHEN N.NOC='FIJ' THEN 'FJI'
// MAGIC                 WHEN N.NOC='FRG' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GDR' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GER' THEN 'DEU'
// MAGIC                 WHEN N.NOC='GRN' THEN 'GRD'
// MAGIC 				WHEN N.NOC='IRI' THEN 'IRN'
// MAGIC 				WHEN N.NOC='MGL' THEN 'MNG'
// MAGIC                 WHEN N.NOC='NED' THEN 'NLD'
// MAGIC 				WHEN N.NOC='NEP' THEN 'NPL'
// MAGIC 				WHEN N.NOC='NGR' THEN 'NGA'
// MAGIC                 WHEN N.NOC='POR' THEN 'PRT'
// MAGIC 				WHEN N.NOC='PUR' THEN 'PRI'
// MAGIC 				WHEN N.NOC='RSA' THEN 'ZAF'
// MAGIC                 WHEN N.NOC='SCG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='SLO' THEN 'SVN'
// MAGIC 				WHEN N.NOC='SUI' THEN 'CHE'
// MAGIC                 WHEN N.NOC='TCH' THEN 'CZE'
// MAGIC 				WHEN N.NOC='TPE' THEN 'TWN'
// MAGIC 				WHEN N.NOC='UAE' THEN 'ARE'
// MAGIC                 WHEN N.NOC='URS' THEN 'RUS'
// MAGIC 				WHEN N.NOC='URU' THEN 'URY'
// MAGIC 				WHEN N.NOC='VIE' THEN 'VNM'
// MAGIC                 WHEN N.NOC='YUG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='ZIM' THEN 'ZWE'
// MAGIC                 WHEN N.NOC='CHI' THEN 'CHL'
// MAGIC                 WHEN N.NOC='GRE' THEN 'GRC'
// MAGIC 				WHEN N.NOC='HAI' THEN 'HTI'
// MAGIC                 WHEN N.NOC='INA' THEN 'IDN'
// MAGIC                 WHEN N.NOC='LAT' THEN 'LVA'
// MAGIC 				ELSE N.NOC
// MAGIC 				END as COUNTRY
// MAGIC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Gold' group by COUNTRY order by Medals desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(Medal) as Medals, case 
// MAGIC 				WHEN N.NOC='ALG' THEN 'DZA'
// MAGIC 				WHEN N.NOC='ANZ' THEN 'AUS'
// MAGIC 				WHEN N.NOC='BAH' THEN 'BHS'
// MAGIC                 WHEN N.NOC='BUL' THEN 'BGR'
// MAGIC 				WHEN N.NOC='CRC' THEN 'CRI'
// MAGIC 				WHEN N.NOC='CRO' THEN 'HRV'
// MAGIC                 WHEN N.NOC='DEN' THEN 'DNK'
// MAGIC 				WHEN N.NOC='EUN' THEN 'RUS'
// MAGIC 				WHEN N.NOC='FIJ' THEN 'FJI'
// MAGIC                 WHEN N.NOC='FRG' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GDR' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GER' THEN 'DEU'
// MAGIC                 WHEN N.NOC='GRN' THEN 'GRD'
// MAGIC 				WHEN N.NOC='IRI' THEN 'IRN'
// MAGIC 				WHEN N.NOC='MGL' THEN 'MNG'
// MAGIC                 WHEN N.NOC='NED' THEN 'NLD'
// MAGIC 				WHEN N.NOC='NEP' THEN 'NPL'
// MAGIC 				WHEN N.NOC='NGR' THEN 'NGA'
// MAGIC                 WHEN N.NOC='POR' THEN 'PRT'
// MAGIC 				WHEN N.NOC='PUR' THEN 'PRI'
// MAGIC 				WHEN N.NOC='RSA' THEN 'ZAF'
// MAGIC                 WHEN N.NOC='SCG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='SLO' THEN 'SVN'
// MAGIC 				WHEN N.NOC='SUI' THEN 'CHE'
// MAGIC                 WHEN N.NOC='TCH' THEN 'CZE'
// MAGIC 				WHEN N.NOC='TPE' THEN 'TWN'
// MAGIC 				WHEN N.NOC='UAE' THEN 'ARE'
// MAGIC                 WHEN N.NOC='URS' THEN 'RUS'
// MAGIC 				WHEN N.NOC='URU' THEN 'URY'
// MAGIC 				WHEN N.NOC='VIE' THEN 'VNM'
// MAGIC                 WHEN N.NOC='YUG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='ZIM' THEN 'ZWE'
// MAGIC                 WHEN N.NOC='CHI' THEN 'CHL'
// MAGIC                 WHEN N.NOC='GRE' THEN 'GRC'
// MAGIC 				WHEN N.NOC='HAI' THEN 'HTI'
// MAGIC                 WHEN N.NOC='INA' THEN 'IDN'
// MAGIC                 WHEN N.NOC='LAT' THEN 'LVA'
// MAGIC                 WHEN N.NOC='AHO' THEN 'CUW'
// MAGIC 				WHEN N.NOC='BOH' THEN 'CZE'
// MAGIC 				WHEN N.NOC='BOT' THEN 'BWA'
// MAGIC                 WHEN N.NOC='GUA' THEN 'GTM'
// MAGIC 				WHEN N.NOC='ISV' THEN 'VGB'
// MAGIC 				WHEN N.NOC='KSA' THEN 'SAU'
// MAGIC                 WHEN N.NOC='LIB' THEN 'LBN'
// MAGIC 				WHEN N.NOC='MAS' THEN 'MYS'
// MAGIC 				WHEN N.NOC='NIG' THEN 'NER'
// MAGIC                 WHEN N.NOC='PAR' THEN 'PRY'
// MAGIC 				WHEN N.NOC='PHI' THEN 'PHL'
// MAGIC                 WHEN N.NOC='SRI' THEN 'LKA'
// MAGIC                 WHEN N.NOC='SUD' THEN 'SSD'
// MAGIC 				WHEN N.NOC='TAN' THEN 'TZA'
// MAGIC                 WHEN N.NOC='TGA' THEN 'TON'
// MAGIC                 WHEN N.NOC='UAR' THEN 'SYR'
// MAGIC                 WHEN N.NOC='ZAM' THEN 'ZMB'
// MAGIC 				ELSE N.NOC
// MAGIC 				END as COUNTRY
// MAGIC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Silver' group by COUNTRY order by Medals desc;

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select count(Medal) as Medals, case 
// MAGIC 				WHEN N.NOC='ALG' THEN 'DZA'
// MAGIC 				WHEN N.NOC='ANZ' THEN 'AUS'
// MAGIC 				WHEN N.NOC='BAH' THEN 'BHS'
// MAGIC                 WHEN N.NOC='BUL' THEN 'BGR'
// MAGIC 				WHEN N.NOC='CRC' THEN 'CRI'
// MAGIC 				WHEN N.NOC='CRO' THEN 'HRV'
// MAGIC                 WHEN N.NOC='DEN' THEN 'DNK'
// MAGIC 				WHEN N.NOC='EUN' THEN 'RUS'
// MAGIC 				WHEN N.NOC='FIJ' THEN 'FJI'
// MAGIC                 WHEN N.NOC='FRG' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GDR' THEN 'DEU'
// MAGIC 				WHEN N.NOC='GER' THEN 'DEU'
// MAGIC                 WHEN N.NOC='GRN' THEN 'GRD'
// MAGIC 				WHEN N.NOC='IRI' THEN 'IRN'
// MAGIC 				WHEN N.NOC='MGL' THEN 'MNG'
// MAGIC                 WHEN N.NOC='NED' THEN 'NLD'
// MAGIC 				WHEN N.NOC='NEP' THEN 'NPL'
// MAGIC 				WHEN N.NOC='NGR' THEN 'NGA'
// MAGIC                 WHEN N.NOC='POR' THEN 'PRT'
// MAGIC 				WHEN N.NOC='PUR' THEN 'PRI'
// MAGIC 				WHEN N.NOC='RSA' THEN 'ZAF'
// MAGIC                 WHEN N.NOC='SCG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='SLO' THEN 'SVN'
// MAGIC 				WHEN N.NOC='SUI' THEN 'CHE'
// MAGIC                 WHEN N.NOC='TCH' THEN 'CZE'
// MAGIC 				WHEN N.NOC='TPE' THEN 'TWN'
// MAGIC 				WHEN N.NOC='UAE' THEN 'ARE'
// MAGIC                 WHEN N.NOC='URS' THEN 'RUS'
// MAGIC 				WHEN N.NOC='URU' THEN 'URY'
// MAGIC 				WHEN N.NOC='VIE' THEN 'VNM'
// MAGIC                 WHEN N.NOC='YUG' THEN 'SRB'
// MAGIC 				WHEN N.NOC='ZIM' THEN 'ZWE'
// MAGIC                 WHEN N.NOC='CHI' THEN 'CHL'
// MAGIC                 WHEN N.NOC='GRE' THEN 'GRC'
// MAGIC 				WHEN N.NOC='HAI' THEN 'HTI'
// MAGIC                 WHEN N.NOC='INA' THEN 'IDN'
// MAGIC                 WHEN N.NOC='LAT' THEN 'LVA'
// MAGIC                 WHEN N.NOC='AHO' THEN 'CUW'
// MAGIC 				WHEN N.NOC='BOH' THEN 'CZE'
// MAGIC 				WHEN N.NOC='BOT' THEN 'BWA'
// MAGIC                 WHEN N.NOC='GUA' THEN 'GTM'
// MAGIC 				WHEN N.NOC='ISV' THEN 'VGB'
// MAGIC 				WHEN N.NOC='KSA' THEN 'SAU'
// MAGIC                 WHEN N.NOC='LIB' THEN 'LBN'
// MAGIC 				WHEN N.NOC='MAS' THEN 'MYS'
// MAGIC 				WHEN N.NOC='NIG' THEN 'NER'
// MAGIC                 WHEN N.NOC='PAR' THEN 'PRY'
// MAGIC 				WHEN N.NOC='PHI' THEN 'PHL'
// MAGIC                 WHEN N.NOC='SRI' THEN 'LKA'
// MAGIC                 WHEN N.NOC='SUD' THEN 'SSD'
// MAGIC 				WHEN N.NOC='TAN' THEN 'TZA'
// MAGIC                 WHEN N.NOC='TGA' THEN 'TON'
// MAGIC                 WHEN N.NOC='UAR' THEN 'SYR'
// MAGIC                 WHEN N.NOC='ZAM' THEN 'ZMB'
// MAGIC 				ELSE N.NOC
// MAGIC 				END as COUNTRY
// MAGIC from athlete_events_final A JOIN noc_regions N ON A.NOC = N.NOC where Medal = 'Bronze' group by COUNTRY order by Medals desc limit 75;
