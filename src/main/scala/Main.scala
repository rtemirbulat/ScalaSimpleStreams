import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleStreamingJob")
      .master("local[*]")
      .getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("date_time", StringType),
      StructField("site_name", IntegerType),
      StructField("posa_continent", IntegerType),
      StructField("user_location_country", IntegerType),
      StructField("user_location_region", IntegerType),
      StructField("user_location_city", IntegerType),
      StructField("orig_destination_distance", DoubleType),
      StructField("user_id", IntegerType),
      StructField("is_mobile", IntegerType),
      StructField("is_package", IntegerType),
      StructField("channel", IntegerType),
      StructField("srch_ci", StringType),
      StructField("srch_co", StringType),
      StructField("srch_adults_cnt", IntegerType),
      StructField("srch_children_cnt", IntegerType),
      StructField("srch_rm_cnt", IntegerType),
      StructField("srch_destination_id", IntegerType),
      StructField("srch_destination_type_id", IntegerType),
      StructField("hotel_id", LongType)
    ))
    //2016
    def sparkReadAvro(spark: SparkSession, filePath: String): DataFrame = {
      val df = spark.read.format("avro")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(filePath)
      return df
    }

    def filterIdAndDates(dataFrame: DataFrame): DataFrame = {
      val df = dataFrame
        .filter(col("srch_ci").isNotNull && col("hotel_id").isNotNull)

      return df
    }

    def createNewStayColumn(df: DataFrame): DataFrame = {
      val duration = df.withColumn("stay",
        datediff(col("srch_co"), col("srch_ci")))
        .filter(col("stay").isNotNull)
      return duration
    }

    def joinByAvgTemp(df: DataFrame, df2: DataFrame, req_year: Int): DataFrame = {
      val newDf2 = df2.withColumnRenamed("wthr_date", "srch_ci")
      val filteredNewDf2 = filterIdAndDates(newDf2)

      val newDf = df.filter(year(col("srch_ci")) === req_year)
      val filteredNewDf1 = filterIdAndDates(newDf)

      val joined = filteredNewDf1
        .join(filteredNewDf2, Seq("hotel_id", "srch_ci"), "inner")
        .select(filteredNewDf1.col("*"), filteredNewDf2.col("address"), filteredNewDf2.col("avg_tmpr_c")
          .alias("weather_avg_tmpr_c"))
        .filter(col("weather_avg_tmpr_c") > 0)
      val result = joined.withColumnRenamed("srch_children_cnt", "with_children")
      return result
    }

    def hotelPreferences(df: DataFrame): DataFrame = {
      val stayType = stayCalculator(col("stay"))
      val currentTime = current_timestamp()
      val batchDFwithTimestamp = df.withColumn("batch_timestamp", currentTime)
      val hotelPreferences = batchDFwithTimestamp
        .groupBy(
          "hotel_id","batch_timestamp"
          ) // by hotel id only
        .agg(
          count("stay").alias("total_count"),
          count(when(stayType === "Erroneous data", true)).alias("erroneous_data_count"),
          count(when(stayType === "Short stay", true)).alias("short_stay_count"),
          count(when(stayType === "Standard stay", true)).alias("standard_stay_count"),
          count(when(stayType === "Standard extended stay", true)).alias("standard_extended_stay_count"),
          count(when(stayType === "Long stay", true)).alias("long_stay_count"),
          functions.max(stayType).alias("most_popular_stay_type"),
          count(when(col("with_children") =!= 0, col("hotel_id")).cast("Integer")).alias("with_children")
        )
      return hotelPreferences.select(
        "hotel_id",
        "with_children",
        "erroneous_data_count",
        "short_stay_count",
        "standard_stay_count",
        "standard_extended_stay_count",
        "long_stay_count",
        "most_popular_stay_type",
        "batch_timestamp"
      )
    }

    def stayCalculator(durationCol: Column): Column = {
      /*err_data: null, d>30, d<=0
               shrt_stay: d==1
               std_stay:  2<=d<=7
               std ext stay : 7<d<=14
               long stay 14<d<=31*/
      when(durationCol.isNull || durationCol <= 0 || durationCol > 30, "Erroneous data")
        .when(durationCol === 1, "Short stay")
        .when(durationCol.between(2, 7), "Standard stay")
        .when(durationCol.between(8, 14), "Standard extended stay")
        .when(durationCol.between(15, 28), "Long stay")
        .otherwise("Erroneous data")
    }


    def mergeExpedia(filePath: String): DataFrame = {
      val ndf1 = sparkReadAvro(spark, filePath + "/expedia-1")
      val ndf2 = sparkReadAvro(spark, filePath + "/expedia-2")
      val ndf3 = sparkReadAvro(spark, filePath + "/expedia-3")
      val mergedDf = ndf1.unionByName(ndf2, true)
      val expediaMerged = mergedDf.unionByName(ndf3, true)
      return expediaMerged
    }

    val hotel_weather2016 = spark.read.format("parquet").load("/home/xl3f/Desktop/SparkStreaming/untitled/dataOutput/hotel-weather/year=2016/*")
      .withColumnRenamed("id", "hotel_id")
    val expedia = mergeExpedia("/home/xl3f/Desktop/SparkStreaming/untitled/dataOutput/expedia")
    val joined = joinByAvgTemp(expedia, hotel_weather2016, 2016)
    val adjustedData = createNewStayColumn(joined)
    val initialState = hotelPreferences(adjustedData)
    val outputPath = "/home/xl3f/Desktop/ScalaSimpleStreams/out/production/historicalData"
    initialState.write.mode("overwrite").parquet(outputPath)
    initialState.show()



    //2017 to do: add timestamp as column
    val streamingDF = spark.readStream
      .format("avro")
      .schema(schema)
      .load("/home/xl3f/Desktop/SparkStreaming/untitled/dataOutput/expedia/*/*")
      .filter(not(col("srch_ci").like("%2016%")) && col("srch_children_cnt") > 0)

    val hotelWeather2017 = spark.read.format("parquet").load("/home/xl3f/Desktop/SparkStreaming/untitled/dataOutput/hotel-weather/year=2017/*/*").withColumnRenamed("id","hotel_id")
    val joinedWithWeather = joinByAvgTemp(streamingDF,hotelWeather2017,2017)
    val joinedWithWeatherStay = createNewStayColumn(joinedWithWeather)
    val aggregatedStream = hotelPreferences(joinedWithWeatherStay)

    // segregate data by adding 2016_ and 2017_ respectively

    val renamedDf = initialState.columns.map(c=>initialState(c).as("2016_"+c))
    val renamedHistorical = initialState.select(renamedDf: _*)
    renamedHistorical.show()

    val renamedDfStream = aggregatedStream.columns.map(c=>aggregatedStream(c).as("2017_"+c))
    val renamedStreaming = aggregatedStream.select(renamedDfStream: _*)

    val joinedAll = renamedStreaming.join(broadcast(renamedHistorical), col("2016_hotel_id") === col("2017_hotel_id")
      , "inner")
      .select(col("2016_hotel_id").alias("hotel_id"),
        (col("2016_short_stay_count") + col("2017_short_stay_count")).alias("short_stay"),
        (col("2016_standard_stay_count") + col("2017_standard_stay_count")).alias("standard_stay"),
        (col("2016_standard_extended_stay_count") + col("2017_standard_extended_stay_count")).alias("standard_extended_stay"),
        (col("2016_long_stay_count") + col("2017_long_stay_count")).alias("long_stay"),
        (col("2016_erroneous_data_count") + col("2017_erroneous_data_count")).alias("erroneous_data_cnt"),
        col("2017_with_children").alias("with_children"),
        (col("2017_batch_timestamp")).alias("batch_timestamp"))

    val result = joinedAll.withColumn("most_popular_stay_type",greatest(
      col("short_stay"),
        col ("standard_stay"),
        col ("standard_extended_stay"),
        col ("long_stay"),
        col ("erroneous_data_cnt")
    ))



    def writeAvro(df: DataFrame, batchId: Long): Unit = {
  //    val currentTime = current_timestamp()
//      val batchDFTimestamp = df.withColumn("batch_timestamp",currentTime)
      df.write
        .format("avro")
        //.format("console")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'")
        .partitionBy("batch_timestamp")
        .option("path", "/home/xl3f/Desktop/ScalaSimpleStreams/out/production/avroBatches")
        .mode("overwrite")
        .save()
    }


    val query = result.writeStream
      .outputMode("complete")
      .format("avro")
      .foreachBatch(writeAvro _)
      .start()
    query.processAllAvailable()
    query.stop()
    spark.stop()

  }
}