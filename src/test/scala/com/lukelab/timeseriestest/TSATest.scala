package com.lukelab.timeseriestest

/**
 * Created by liyu on 9/26/15.
 */

import java.text.SimpleDateFormat
import java.util.Date

import com.lukelab.timeseries.{TimeOccurrence, TimeSeriesAnalysis}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.junit.{After, Before}

class TSATest {
  @transient var sc: SparkContext = _

  @Before
  def setUp(): Unit = {
    val conf = new SparkConf().setAppName("anomaly detection unit test")
    if (!conf.contains("spark.master"))
      conf.setMaster("local")

    sc = new SparkContext(conf)
  }

  @After
  def tearDown(): Unit = {
    if (sc != null) {
      sc.stop()
    }
    sc = null
  }

  @org.junit.Test
  def testGenerateTimeSeriesData(): Unit ={
    val data: RDD[(Date, Double)] = TimeSeriesAnalysis.generateTimeSeriesData(sc, "./data/tsdata.csv")
    assert (data.count() == 766)
  }

  @org.junit.Test
  def testMedian(): Unit = {
    val data = sc.parallelize(Array[Double](1, 2, 3, 4, 5))
    val median = TimeSeriesAnalysis.computeMedian(data)
    assert(median == 3)

    val data2 = sc.parallelize(Array[Double](1, 2, 3, 4, 5, 6))
    val median2 = TimeSeriesAnalysis.computeMedian(data2)
    assert(median2 == 3.5)
    val median3 = TimeSeriesAnalysis.computeMedian(data2, true)
    assert(median3 == 3.5)
  }

  @org.junit.Test
  def testMAD(): Unit = {
    val data = sc.parallelize(Array[Double](1, 2, 3, 4, 5))
    val mad = TimeSeriesAnalysis.computeMedianAbsoluteDeviation(data)
    assert(mad == 1.4826)
    val mad2 = TimeSeriesAnalysis.computeMedianAbsoluteDeviation(data,
      medianVal = 3)
    assert(mad2 == 1.4826)
  }

  @org.junit.Test
  def testDetectAnomaly(): Unit = {
    sc.setCheckpointDir("checkpoints")
    val data: RDD[(Date, Double)] =
      sc.textFile("./data/tsdata.csv").map { line =>
        val values = line.split(",")
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH");
        val ts = sdf.parse(values(0));
        val cnt = values(1).toDouble
        (ts, cnt)
      }

    val anoms = TimeSeriesAnalysis.detectAnomaly(data,
      stlFilter = TimeSeriesAnalysis.filterSeasonalData)

    val anomsTs = anoms.map (to => to.ts)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH");
    var ts = sdf.parse("2015-03-04 22:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-02-27 01:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-03 00:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-30 00:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-28 23:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-12 04:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-06 23:00:00");
    assert(anomsTs.contains(ts))
    ts = sdf.parse("2015-03-03 23:00:00");
    assert(anomsTs.contains(ts))

  }

  @org.junit.Test
  def testParseArgument(): Unit = {
    val argumentMap: Map[String, String] =
      TimeSeriesAnalysis.parseArguments(Array("--tenantId", "abc",
        "--dataFile", "./data/appusage/itapsamples.csv"))
    assert(argumentMap.size == 2)


  }
}
