import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf().setAppName("BikeDataAnalysis").setMaster("local[*]")
val sc = new SparkContext(conf)

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("BikeDataAnalysis").getOrCreate()
val data = spark.sparkContext.textFile("/home/tarunesh38/Downloads/WhatSie/Used_Bikes.csv")
//val bikeData = sc.textFile("C:/Users/tarun/OneDrive/Documents/Sem4/BDA/Used_Bikes.csv")
val header = data.first()
val bikeData = data.filter(rec => rec != header)

// 1) What is the average price of used bikes in India?
val bikePrices = bikeData.map(_.split(",")).map(fields => fields(1).toDouble)
val averagePrice = bikePrices.mean()
println(f"Average price of used bikes in India: ₹${averagePrice}%.2f")

// 2) How does the price of used bikes vary across different cities in India?
val cityPrices = bikeData.map(_.split(",")).map(fields => (fields(2), fields(1).toDouble))
val avgPriceByCity = cityPrices.groupByKey().mapValues(prices => prices.sum / prices.size)
avgPriceByCity.toDF("City", "Average Price").show(false)
//avgPriceByCity.collect().foreach { case (city, price) => println(f"Average price in $city: ₹${price}%.2f") }

// 3) Which bike brand has the highest average price?
val brandPrices = bikeData.map(_.split(",")).map(fields => (fields(0), fields(1).toDouble))
val avgPriceByBrand = brandPrices.groupByKey().mapValues(prices => prices.sum / prices.size)
val highestAvgPriceBrand = avgPriceByBrand.max()(Ordering.by(_._2))
println("Bike brand with the highest average price: " + highestAvgPriceBrand._1)

// 4) What is the distribution of bike prices in the dataset?
val bikePrices = bikeData.map(_.split(",")).map(fields => fields(1).toDouble)
val priceDistribution = bikePrices.histogram(10)
println("Price distribution: " + priceDistribution._1.mkString(", "))
println("Counts: " + priceDistribution._2.mkString(", "))

// 5) Is there a correlation between the age of a bike and its price?
val agePrice = bikeData.map(_.split(",")).map(fields => (fields(5).toDouble, fields(1).toDouble))
val correlation = agePrice.groupByKey().mapValues(prices => prices.sum / prices.size)
correlation.toDF("Age","Price").show(false)
//correlation.collect().foreach(println)

// 6) Which bike' power segment has the highest average price?
val segmentPrices = bikeData.map(_.split(",")).map(fields => (fields(6), fields(1).toDouble))
val priceCounts = bikePrices.map(price => (1, price)).countByKey()
val avgPriceBySegment = segmentPrices.reduceByKey(_ + _).mapValues(total => total / priceCounts(1))
val highestAvgPriceSegment = avgPriceBySegment.max()(Ordering.by(_._2))
println("Bike segment with the highest average price: " + highestAvgPriceSegment._1)

// 7) Are there any specific bike models that tend to retain their value better than others?
val modelPrices = bikeData.map(_.split(",")).map(fields => (fields(0), fields(1).toDouble))
// Compute average price by model
val avgPriceByModel = modelPrices.mapValues((_, 1)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues { case (total, count) => total / count } // Compute average price for each model
// Find the model with the highest average price
val highestAvgPriceModel = avgPriceByModel.max()(Ordering.by[(String, Double), Double](_._2))
// Print the bike model that retains value better than others
println("Bike model that retains value better than others: " + highestAvgPriceModel._1)


// 8) To find the Power of Yamaha Bikes which means it has the engine power as greater than 150cc and only handled by first owner.
val splittedRdd = bikeData.map(line => line.split(",").map(column => column.trim))
val resultRDD = splittedRdd.filter(bike => bike(4).equalsIgnoreCase("First Owner") && bike(6).toDouble > 150 && bike(7).equalsIgnoreCase("Yamaha"))
// Extract bike and owner pairs, remove duplicates, and display the result
val distinctBikeOwnerPairs = resultRDD.map(bike => (bike(0), bike(4))).distinct()
distinctBikeOwnerPairs.toDF("Bike", "Owner").show(false)
//resultRDD.map(bike => (bike(0), bike(4))).distinct().foreach(println)
//println ("Count of the final Datasets " + resultRDD.count())

// 9) The person should be from Mumbai and should be a Second Owner.
val resultRDD = splittedRdd.filter(bike => bike(4).equalsIgnoreCase("Second Owner") && bike(2).equalsIgnoreCase("Mumbai"))
val MumbaiSecondRDD = resultRDD.map(bike => (bike(0), bike(4), bike(2))).distinct()
MumbaiSecondRDD.toDF("Bike", "Owner","City").show(false)
//resultRDD.map(bike => (bike(0), bike(4), bike(2))).distinct().foreach(println)
