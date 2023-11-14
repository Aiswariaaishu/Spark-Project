package spark.utilities
import org.apache.spark.sql.functions.{col, concat_ws, count, date_format, desc, first, format_number, lit, max, min, round, split, substring, sum, to_date, to_timestamp, udf, when, year}
import org.apache.spark.sql.types.StringType

object finalproject extends SparkApp with App{

  val path = "src/main/resources/project_dataset/phu_locations.json"
  val phuDf = spark.read.json(path)
  phuDf.show(10,false)
  //phuDf.printSchema()

  val path2 = "src/main/resources/project_dataset/cases_by_status_and_phu.csv"
  val casesDf = spark.read.option("header",true).option("inferSchema","true").csv(path2)
  casesDf.show(5,false)
  //casesDf.printSchema()

  //1: list all PHU locations which are allow online appointments.List should not contain columns in French.All French columns have name ending with fr.
  phuDf.select("PHU", "location_name", "online_appointments").filter(col("online_appointments").isNotNull).show(5,false)

  //Task 2 : List all the PHU locations allow children under 2 in Brampton location.
  phuDf.select("PHU", "city","children_under_2").filter(col("children_under_2") === "Yes" && col("city") === "Brampton").show(false)

  //Task 3: List all the PHU locations in Toronto has free parking with drive through.
  phuDf.select("PHU","city","drive_through","free_parking").filter(col("city") === "Toronto" && col("drive_through") === "Yes" && col("free_parking") === "Yes").show(false)

  /*phuDf.filter(col("city") === "Toronto" && col("free_parking") === "Yes" && col("drive_through") === "Yes")
    .select("PHU", "location_name", "free_parking", "drive_through")
    .show(false)*/


  //phuDf.select("friday","city","PHU").filter(col("friday").isNull).show(false)

  //Task 4 : Find out number of PHUs are closed in each city on Friday

  phuDf.filter(col("friday").isNull || col("friday") === "No" )
    .groupBy("city")
    .agg(count("*").alias("PHU_Count_Closed_Friday"))
    .show(false)

 /* val selectedColumns = phuDf.columns.filter(!_.endsWith("_fr"))
  val dfWithoutExcludedColumns = phuDf.select(selectedColumns.head, selectedColumns.tail: _*)
  dfWithoutExcludedColumns.show(false)*/

  //Task 5 : find out all the PHU locations which are open during 20:00 and 21:00 on Monday in Toronto city.

  phuDf.createOrReplaceTempView("phutable")
  phuDf.select("PHU","city","monday")
       .where((col("monday").like("%20:00") )|| (col("monday").like("%21:00")))
       .where(col("city") === "Toronto").show(false)


  //spark.sql("SELECT PHU, city, monday FROM phutable WHERE (monday LIKE '%20:00' OR monday LIKE '%21:00') AND (city = 'Toronto') AND monday IS NOT NULL").show(false)

  //Task 6 : List the hours of operation of all the PHU in Toronto. Use below format.

  spark.sql("SELECT PHU, drive_through, free_parking FROM phutable WHERE city = 'Toronto' AND (drive_through = 'Yes') AND (free_parking = 'YES')").show(false)

  //phuDf.select("PHU","monday","tuesday","wednesday","thursday","friday","saturday","sunday").show(false)

  //Task 6 : List the hours of operation of all the PHU in Toronto. Use below format.
  phuDf
    .select(
      col("PHU"), col("monday").as("Monday"),col("tuesday").as("Tuesday"),
      col("wednesday").as("Wednesday"),col("thursday").as("Thursday"),
      col("friday").as("Friday"),col("saturday").as("Saturday"),col("sunday").as("Sunday"))
    .withColumn("Monday", concat_ws(" to ", split(col("Monday"), "-")))
    .withColumn("Tuesday", concat_ws(" to ", split(col("Tuesday"), "-")))
    .withColumn("Wednesday", concat_ws(" to ", split(col("Wednesday"), "-")))
    .withColumn("Thursday", concat_ws(" to ", split(col("Thursday"), "-")))
    .withColumn("Friday", concat_ws(" to ", split(col("Friday"), "-")))
    .withColumn("Saturday", concat_ws(" to ", split(col("Saturday"), "-")))
    .withColumn("Sunday", concat_ws(" to ", split(col("Sunday"), "-")))
    .show(false)

  //Task 7:Count all the PHU are temporary closed in each city.

  phuDf.filter(col("temporarily_closed") === "Yes")
    .select("city", "temporarily_closed", "PHU")
    .groupBy("city")
    .agg(count("PHU").alias("PHU_count"))
    .show(phuDf.count().toInt, false)

  //phuDf.select("temporarily_closed").show(phuDf.count().toInt, false)

  /*phuDf.select("city", "temporarily_closed","PHU")
      .groupBy("city")
      .agg(count("PHU").alias("PHU_count"))
      .show(phuDf.count().toInt, false)
*/
  //Task 8:phu_locations.json has phone field. Create two more columns. First field should contain only area code and 2nd field should contain extension number.

  val phuphDf1 = phuDf.select("phone")
                      .withColumn("area_code",substring(col("phone"),1,3))
                      .withColumn("extension",substring(col("phone"),19,22))
  phuphDf1.show(phuphDf1.count().toInt,false)



  //Task 9:Find out highest number of resolved cases of covid-19 in each PHU between April 2020 to Sep 2020

  val formattedDf = casesDf.select("PHU_NAME","FILE_DATE","RESOLVED_CASES").withColumn("timestamp_date", to_timestamp(col("FILE_DATE").cast(StringType), "yyyyMMdd"))
  val casedf1 = formattedDf.select(col("timestamp_date"),col("PHU_NAME"),col("FILE_DATE"),col("RESOLVED_CASES"), date_format(col("timestamp_date"), "MMMMyyyy").as("updated_date"))
  casedf1.show(false)

  casedf1
    .filter(col("updated_date").between("April2020", "Sep2020"))
    .groupBy("PHU_NAME")
    .agg(max("RESOLVED_CASES").alias("HIGHEST_RESOLVED_CASES")).show(false)

  //Task 10 : which PHU has more resolved cases of covid-19 than active cases in year 2020.

  val yearDf = casesDf.select("FILE_DATE","PHU_NAME","ACTIVE_CASES","RESOLVED_CASES")
    .withColumn("timestamp_year",to_timestamp(col("FILE_DATE").cast(StringType),"yyyyMMdd"))
  val updatedyearDf = yearDf.select(col("timestamp_year"),col("PHU_NAME"),col("ACTIVE_CASES"),col("RESOLVED_CASES"),date_format(col("timestamp_year"),"yyyy").as("YEAR"))
  updatedyearDf.show(false)

  updatedyearDf.filter(col("YEAR") === "2020")
    .filter(col("RESOLVED_CASES") > col("ACTIVE_CASES"))
    .select("YEAR", "PHU_NAME", "RESOLVED_CASES","ACTIVE_CASES")
    .orderBy(desc("RESOLVED_CASES"))
    .show(1,false)

  //Task 11 : List only PHU which has least death due to covid-19 in each month.

  val monthDf = casesDf.select("FILE_DATE", "PHU_NAME","DEATHS")
    .withColumn("timestamp_month", to_timestamp(col("FILE_DATE").cast(StringType), "yyyyMMdd"))
  val updatedmonthDf = monthDf.select(col("timestamp_month"), col("PHU_NAME"), col("DEATHS"), date_format(col("timestamp_month"), "MMMM").as("MONTH"))
  updatedmonthDf.show(false)

  updatedmonthDf.select("PHU_NAME","MONTH","DEATHS")
                .groupBy("MONTH")
                .agg(min("DEATHS").alias("MIN_DEATH")).show(false)

  updatedmonthDf.select("PHU_NAME", "MONTH", "DEATHS")
    .groupBy("MONTH")
    .agg(min("DEATHS").alias("MIN_DEATH"))
    .withColumn("MIN_DEATH", when(col("MIN_DEATH") > 0, true).otherwise(false))
    .show(false)

  updatedmonthDf.select("PHU_NAME", "MONTH", "DEATHS")
    .groupBy("MONTH")
    .agg(min("DEATHS").alias("MIN_DEATH"))
    .filter(col("MIN_DEATH") > 0)
    .show(false)

  //updatedmonthDf.select("DEATHS","MONTH").filter(col("MONTH") === "April" && col("DEATHS") === 0).show(false)

  //Task 12 : Find out total active cases remaining in each PHU

  casesDf.select("ACTIVE_CASES","PHU_NAME").show(casesDf.count().toInt,false)
  casesDf.select("ACTIVE_CASES","PHU_NAME")
         .groupBy("PHU_NAME")
         .agg(sum("ACTIVE_CASES").alias("TOTAL_ACTIVE_CASES")).show(casesDf.count().toInt,false)

  //Task 13 : Find out on which date highest and lowest death reported in “NIAGARA REGION” PHU.

  casesDf.select("FILE_DATE", "PHU_NAME", "DEATHS")

    .filter(col("PHU_NAME") === "NIAGARA REGION")
    .agg(
      min("DEATHS").alias("MIN_DEATHS"),
      max("DEATHS").alias("MAX_DEATHS"),
      first("FILE_DATE").alias("FILE_DATE"),
      first("PHU_NAME").alias("PHU_NAME")
    )
    .show(false)

  //Task 14 : Find out total resolved cases of covid-19 in “NORTH BAY PARRY SOUND DISTRICT” in month May 2020 and Oct 2020.

  /*val casedf = casesDf
    .select("FILE_DATE","PHU_NAME","RESOLVED_CASES")
    .withColumn("timestamp_date",to_timestamp(col("FILE_DATE").cast(StringType),"yyyyMMdd"))

  val formated_df = casedf.select(col("timestamp_date"),col("PHU_NAME"),col("RESOLVED_CASES"),date_format(col("timestamp_date"),"MMMM yyyy").as("MONTH"))
  formated_df.show(false)

  formated_df.select("MONTH","PHU_NAME","RESOLVED_CASES")
    .filter(col("PHU_NAME") === "NORTH BAY PARRY SOUND DISTRICT")
    .filter(col("MONTH").between("MAY 2020","October 2020"))
    .agg(sum("RESOLVED_CASES").alias("TOTAL_RESOLVED_CASES"),first(col("MONTH").alias("MONTH"))).show(false)*/

  val casedf = casesDf
    .select("FILE_DATE", "PHU_NAME", "RESOLVED_CASES")
    .withColumn("timestamp_date", to_timestamp(col("FILE_DATE").cast(StringType), "yyyyMMdd"))

  val formatted_df = casedf
    .select(col("timestamp_date"), col("PHU_NAME"), col("RESOLVED_CASES"), date_format(col("timestamp_date"), "MMMMyyyy").as("MONTH"))

  formatted_df.show(false)

  formatted_df
    .select("MONTH", "PHU_NAME", "RESOLVED_CASES")
    .filter(col("PHU_NAME") === "NORTH BAY PARRY SOUND DISTRICT")
    .filter(col("MONTH").isin("May2020", "October2020"))
    .groupBy("MONTH")
    .agg(sum("RESOLVED_CASES").alias("TOTAL_RESOLVED_CASES"))
    .show(false)

  //: Find out percentage of active cases in each PHU in year 2020. Result should be link below.

 /* val resultDf = casesDf
    .withColumn("YEAR", year(to_date(col("FILE_DATE").cast(StringType), "yyyyMMdd")))
    .withColumn("TOTAL_CASES", col("ACTIVE_CASES") + col("RESOLVED_CASES"))
    .withColumn("TOTAL_PERCENTAGE", round((col("TOTAL_CASES") / col("ACTIVE_CASES")) * 100, 2))
    .filter(col("YEAR") === "2020")
    .groupBy("YEAR", "PHU_NAME")
    .agg(max("TOTAL_PERCENTAGE").alias("MAX_TOTAL_PERCENTAGE"))
    .select("YEAR", "PHU_NAME", "MAX_TOTAL_PERCENTAGE")

  resultDf.show(false)*/

  val resultDf = casesDf
    .withColumn("YEAR", year(to_date(col("FILE_DATE").cast("string"), "yyyyMMdd")))
    .withColumn("TOTAL_CASES", col("ACTIVE_CASES") + col("RESOLVED_CASES"))
    .withColumn("TOTAL_PERCENTAGE", (col("TOTAL_CASES") / col("ACTIVE_CASES")) * 100)
    .filter(col("YEAR") === "2020")
    .groupBy("YEAR", "PHU_NAME")
    .agg(max("TOTAL_PERCENTAGE").alias("MAX_TOTAL_PERCENTAGE"))

  val formattedResultDf = resultDf.withColumn("formatted_percentage", format_number(col("MAX_TOTAL_PERCENTAGE"), 2))
    .select(col("PHU_NAME"), concat_ws("\t", col("formatted_percentage"), lit("%")).as("total_percentage"))

  formattedResultDf.show(false)



}
