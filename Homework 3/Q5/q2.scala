// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.
val customSchema2 = StructType(Array( StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))

// ENTER THE CODE BELOW

val df2 = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema2)
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with


// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val df1a = df_filter.groupBy("DOLocationID").count().withColumnRenamed("count","number_of_dropoffs")
.withColumn("number_of_dropoffs",col("number_of_dropoffs").cast("int")).orderBy(desc("number_of_dropoffs"),asc("DOLocationID"))
df1a.show(5)

// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val df1b = df_filter.groupBy("PULocationID").count().withColumnRenamed("count","number_of_pickups")
.withColumn("number_of_pickups",col("number_of_pickups").cast("int")).orderBy(desc("number_of_pickups"),asc("PULocationID"))
df1b.show(5)

// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
val dfp2 = df1a.join(df1b, $"DOLocationID" === $"PULocationID","inner").withColumnRenamed("DOLocationID","LocationID").withColumn("number_activities", col("number_of_pickups") + col("number_of_dropoffs")).select("LocationID","number_activities")
.withColumn("number_activities",col("number_activities").cast("int")).orderBy(desc("number_activities"),asc("LocationID"))
dfp2.show(3)

//.agg(sum(col("number_of_pickups"), col("number_of_dropoffs")).as("number_activities")).show()

// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
val dfp3 = dfp2.join(df2, dfp2.col("LocationID") === df2.col("LocationID"),"inner").groupBy("Borough").agg(sum("number_activities").as("number_activities"))
.withColumn("number_activities",col("number_activities").cast("int")).orderBy(desc("number_activities"))
dfp3.show()

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
val df_day1 = df_filter.withColumn("day_of_week",date_format(col("pickup_datetime"), "EEEE")).withColumn("datetype", to_date(col("pickup_datetime"), "yyyy-MM-dd"))


val df_day2 = df_day1.select("day_of_week", "datetype")

val df_day3 = df_day2.groupBy("datetype").agg(count("datetype").alias("count")).orderBy(desc("count"))

val df_day4 = df_day3.withColumn("day_of_week", date_format(col("datetype"),"EEEE"))

val df_day = df_day4.groupBy("day_of_week").agg(avg("count").alias("avg_count")).orderBy(desc("avg_count"))

display(df_day)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
val df_brk = df_filter.join(df2, df_filter.col("PULocationID") === df2.col("LocationID"),"inner").withColumn("hour_of_day",date_format(col("pickup_datetime"), "HH")).where(col("Borough")=== "Brooklyn").groupBy("hour_of_day","zone","LocationID").count() 
//.drop("pickup_datetime","dropoff_datetime","PULocationID","DOLocationID","passenger_count","trip_distance","fare_amount","payment_type" )
val window1 = Window.partitionBy("hour_of_day").orderBy(asc("hour_of_day")) //.groupBy("PULocationID").count()
val window2 = Window.partitionBy("LocationID").orderBy(desc("LocationID"))

val df_brook = df_brk.withColumn("row",row_number.over(window1))
.withColumn("max_count", max((col("count"))).over(window2))
.where(col("row")===1).withColumn("hour_of_day",col("hour_of_day").cast("int")).withColumn("max_count",col("max_count").cast("int"))
.select("hour_of_day","zone","max_count").orderBy(asc("hour_of_day"))

df_brook.show(24)

// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW
val df_brook1 = df_filter.join(df2, df_filter.col("PULocationID") === df2.col("LocationID"),"inner").withColumn("day",date_format(col("pickup_datetime"), "DD")).withColumn("Month",date_format(col("pickup_datetime"), "MM")).where(col("Month")==="01" && col("Borough") === "Manhattan").groupBy("day","Month","Borough","LocationID").count()//.select("day","Borough","count").groupBy("day").agg(sum("count")).orderBy(asc("day"))//.show(31)

val window3 = Window.partitionBy("day").orderBy(asc("day"))
val window4 = Window.partitionBy("sum(count)").orderBy(asc("day"))

val df_brook2 = df_brook1//.withColumn("row",row_number.over(window3))
.withColumn("lag", lag("count",1).over(window3))
.select("day","Borough","count","lag").groupBy("day").agg(sum("count"),sum("lag")).orderBy(asc("day"))
.withColumn("percent_cha",((col("sum(count)") - col("sum(lag)"))/col("sum(count)"))).withColumn("percent_change",round(col("percent_cha"),2))
.withColumn("day",col("day").cast("int"))
.select("day","percent_change").orderBy(desc("percent_change"))

df_brook2.show(3)

// COMMAND ----------


