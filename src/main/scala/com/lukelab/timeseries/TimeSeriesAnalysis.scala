package com.lukelab.timeseries

/**
 * Created by liyu on 9/25/15.
 */

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import com.lukelab.io.{MongoDataSink, TimeSeriesDataSink}

//import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
//import com.mongodb._

import scalation.random.Quantile

object TimeSeriesAnalysis extends Logging {
  private val dataFileParam = "--dataFile"
  private val stlParam = "--stlEnabled"
  private val dbUrlParam = "--dbUrl"
  private val defaultDBUrl = "mongodb://localhost:27017/timeseries"
  private val anomalyCollection = "TimeSeriesAppAnomaly"
  private val timeSeriesDataCollection = "TimeSeriesData"
  private val tenantIdParam = "--tenantId"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("anomaly detection app")

    if (!conf.contains("spark.master"))
      conf.setMaster("local")
    //if (!conf.contains("spark.app.name"))
    //  conf.setAppName("time series anomaly detection")

    val sc = new SparkContext(conf)

    val argumentMap = parseArguments(args)

    val tenantId =
      argumentMap.getOrElse(tenantIdParam, "")

    if("".equals(tenantId)) {
      logError("Tenant Id needs to be provided as an argument e.g. --tenantId companyABC.")
      return
    }

    sc.setCheckpointDir("checkpoints")
    if (argumentMap.contains(dataFileParam)) {
      val dataFile = argumentMap(dataFileParam)
      val data = generateTimeSeriesData(sc, dataFile)
      data.cache()


      val stlfilter:RDD[(Date, Double)] => RDD[(Date, Double)] =
        if(argumentMap.contains(stlParam) && argumentMap(stlParam).toLowerCase.equals("true"))
        {
          filterSeasonalData
        }
        else {
          identity
        }
      val anoms = detectAnomaly(data,
        stlFilter = stlfilter)

      //database operations here
      val dbUrl =
        argumentMap.getOrElse(dbUrlParam, defaultDBUrl)
      val dbName = dbUrl.substring(dbUrl.lastIndexOf("/")+1)

      //saveTimeSeriesData(client, dbName, timeSeriesDataCollection, tenantId,
      //  data.collect)
      val anomTuples = anoms.map { to =>
        (to.ts, to.occurrence)
      }
      val dataSink: TimeSeriesDataSink = new MongoDataSink(dbUrl, tenantId, dbName, anomalyCollection)
      dataSink.save(anomTuples)
    }
    else {
      log.warn("No data file provided.")


    }
  }

  def parseArguments(args: Array[String]): Map[String, String] = {
    val commandArgs = for(i <- (0 until args.size by 2)) yield args(i)
    val valueArgs = for(i <- (1 until args.size by 2)) yield args(i)
    require (commandArgs.size == valueArgs.size, "The number of parameters does not match that of values.")

    val commandValuePairs = commandArgs.zip(valueArgs)
    commandValuePairs.toMap
  }

  def computeMedian(data: RDD[Double], sorted: Boolean = false): Double = {
    val sortedData = if(sorted) {
      data.zipWithIndex().map {
        case (value, index) => (index, value)
      }
    }
    else {
      data.sortBy(identity).zipWithIndex().map {
        case (value, index) => (index, value)
      }
    }

    val count = sortedData.count()
    if(count % 2 ==0) {
      val l = count/2 - 1
      val r = l + 1
      (sortedData.lookup(l).head + sortedData.lookup(r).head).toDouble / 2
    }
    else sortedData.lookup(count/2).head.toDouble
  }

  def computeMedianAbsoluteDeviation(data: RDD[Double], constant: Double = 1.4826, medianVal: Double = Double.PositiveInfinity): Double = {
    val median = if(medianVal == Double.PositiveInfinity) computeMedian(data) else medianVal
    val residual = data.map { x =>
      Math.abs(x - median)
    }
    constant * computeMedian(residual)
  }

  def generateTimeSeriesData(sc: SparkContext, filePath: String): RDD[(Date, Double)] = {
    val rawdata = sc.textFile(filePath).map { line =>
      line.split(",")
    }
    rawdata.map { values =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH");
      val ts = sdf.parse(values(0));
      (ts, values(1).toDouble)
    }.sortByKey()
  }

  def filterSeasonalData(data: RDD[(Date, Double)]) : RDD[(Date, Double)] = {

    val hourMedians: Array[Double] = new Array(24)
    for(h <- 0 to 23) {
      val hourData = data.filter { case (ts, occurrence) =>
        val cal = Calendar.getInstance()
        cal.setTime(ts)
        val wkday = cal.get(Calendar.DAY_OF_WEEK)
        (ts.getHours == h) && (wkday != Calendar.SATURDAY && wkday != Calendar.SATURDAY)
      }.map { case (ts, occurrence) => occurrence}
      hourMedians(h) = computeMedian(hourData)
    }
    data.map {case (ts, occurrence) =>
      (ts, occurrence - hourMedians(ts.getHours))
    }

  }

  def detectAnomaly(data: RDD[(Date, Double)], k: Double = 0.49, alpha: Double = 0.05, stlFilter: RDD[(Date, Double)] => RDD[(Date, Double)] = identity): Array[TimeOccurrence] = {
    val decomposedData = stlFilter(data)
    val occurrenceData = decomposedData.map {case (ts, cnt) => cnt}
    val median = computeMedian(occurrenceData)

    // Remove the seasonal component, and the median of the data to create the univariate remainder
    val residualData = decomposedData.map {case (ts, cnt) => TimeOccurrence(ts, (cnt - median).toInt)}
    val numObs = decomposedData.count()
    val maxOutliers = (numObs * k).toInt
    assume (maxOutliers > 0, "max outliers need to be greater than 0.")

    var sortedResidualData = residualData.sortBy(to => to.occurrence)
    var numAnoms = 0;
    val RBuffer: ArrayBuffer[TimeOccurrence] = ArrayBuffer()

    // Compute test statistic until r=max_outliers values have been
    // removed from the sample.
    var continue = true
    var i = 1
    do {
      val observations = sortedResidualData.map (to => to.occurrence)
      val medianVal = computeMedian(observations, true)
      val madVal = computeMedianAbsoluteDeviation(observations, medianVal = medianVal)
      assume(madVal != 0, "madVal cannot be 0")

      if (i % 5 == 0)
        observations.checkpoint()

      //val ares = sortedResidualData.map (to => TimeOccurrence(to.ts, Math.abs(to.occurrence - medianVal)/madVal))
      val ares = sortedResidualData.map (to => TimeOccurrence(to.ts, (to.occurrence - medianVal)/madVal))
      val R = ares.reduce ((to1, to2) =>
        if(to1.occurrence > to2.occurrence) to1 else to2
      )

      sortedResidualData = sortedResidualData.filter{ to =>
        to.ts != R.ts
      }

      //compute critical value.
      val p = 1 - alpha/(numObs-i+1)

      val t = Quantile.studentTInv(p, (numObs-i-1).toInt)
      val lam = t*(numObs - i)/math.sqrt((numObs-i-1+t*t)*(numObs-i+1))

      if(R.occurrence <= lam)
        continue = false
      else {
        RBuffer += TimeOccurrence(R.ts, data.lookup(R.ts)(0))
      }
      i = i+1
    }  while (continue && i < maxOutliers)

    RBuffer.toArray
  }
}

