package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.Constants.BIDS_HEADER
import com.epam.hubd.spark.scala.core.homework.domain.{
  BidError,
  BidItem,
  EnrichedItem
}
import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(
      args.length == 4,
      "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    System.setProperty("hadoop.home.dir", "c:\\")

//    val spark = SparkSession.builder
//      .master("local[2]")
//      .appName("motels-home-recommendation")
//      .getOrCreate()
//
//    val sc = spark.sparkContext

    val sc = new SparkContext(
      new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext,
                  bidsPath: String,
                  motelsPath: String,
                  exchangeRatesPath: String,
                  outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] =
      getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath)
      .map(_.split("\n").toList)
      .map(list => list.flatMap(_.split(Constants.DELIMITER)))
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids
      .filter(list => list(2).contains("ERROR"))
      .map(str => BidError(str(1), str(2)))
      .map(bidError => (bidError, 1))
      .reduceByKey(_ + _)
      .map(m => m._1.toString + "," + m._2)
  }

  def getExchangeRates(sc: SparkContext,
                       exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath)
      .map(_.split("\n"))
      .map(arr => arr.flatMap(_.split(Constants.DELIMITER)))
      .map(arr => arr(0) -> arr(3).toDouble)
      .collect()
      .toMap[String, Double]
  }

  def getBids(rawBids: RDD[List[String]],
              exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val exchangesDates = exchangeRates.map(m =>
      DateTime.parse(m._1, Constants.INPUT_DATE_FORMAT) -> m._2)

    rawBids
      .filter(list => !list(2).contains("ERROR") && list.size > 8)
      .flatMap(list => {
        val bidData = DateTime.parse(list(1), Constants.INPUT_DATE_FORMAT)
        val outputData = bidData.toString(Constants.OUTPUT_DATE_FORMAT)

//        val exchanges = exchangesDates
//          .filter(m => bidData.isAfter(m._1) || bidData.isEqual(m._1))
//          .reduceLeft((d1, d2) => if (d1._1.isAfter(d2._1) || d1._1.isEqual(d2._1)) d1 else d2)
//
//        if(list(0).contains("0000005") && list(1).contains("01-13-05-2016"))
//          println(exchanges._1.toString() + " " + exchanges._2 + " " + list(8) + " " + (exchanges._2 * list(8).toDouble).formatted("%.3f"))

        val exchangesRate = exchangesDates
          .filter(m => bidData.isAfter(m._1) || bidData.isEqual(m._1))
          .reduceLeft((d1, d2) => if (d1._1.isAfter(d2._1) || d1._1.isEqual(d2._1)) d1 else d2)._2

        if(list(5) == "" && list(6) == "" && list(8) == "") {
          List[BidItem]()
        }
        else {
          val bid = List(5,8,6).flatMap(num =>
            if (!list(num).equals("")) {
              List[BidItem](BidItem(list.head, outputData, BIDS_HEADER(num), list(num).toDouble * exchangesRate))
            } else {
              List[BidItem]()
            }
          ).maxBy(_.price)
          List(bid)
        }
      })
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath)
      .map(_.split("\n"))
      .map(str => str.flatMap(_.split(Constants.DELIMITER)))
      .map(arr => (arr(0), arr(1))).foreach(c => println(c._1 + " " + c._2))

    sc.textFile(motelsPath)
      .map(_.split("\n"))
      .map(str => str.flatMap(_.split(Constants.DELIMITER)))
      .map(arr => (arr(0), arr(1)))
  }

  def getEnriched(bids: RDD[BidItem],
                  motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val broadcastedMotels =
      motels.map(item => item._1 -> item._2).collect().toMap

    bids.map(item => {
      val motelName = broadcastedMotels.getOrElse(item.motelId, "unknown")
      EnrichedItem(item.motelId, motelName, item.bidDate, item.loSa, item.price)
    })
  }
}
