# TimeSeriesAnalysis
This repository contains a bunch of implementation of time series data analytics including anomaly detection, prediction and etc.

Time Series Anomaly Detection

This is a spark implementation based on twitter's paper: https://www.usenix.org/system/files/conference/hotcloud14/hotcloud14-vallis.pdf
The basic idea is to use robust statistical metrics (Median Abosulte Deviation) to discover outliers in the long-term time series data. Up to now, the STL trending is not implemented the same way as in the paper.

The input file contains time series data in the format of:

timestamp1 value1

timestamp2 value2

timestamp3 value3

...

The result is a list of outliers detected. The result will be stored in collection TimeSeriesAppAnomaly of the MongoDB. This will enable correlating timeseries outliers with other data as the next step.

To run the application, first assemble a fat jar file: sbt clean assembly.

Then install and start a local mongod: http://docs.mongodb.org/getting-started/shell/installation/

Submit your spark job: <spark home>/bin/spark-submit --class "com.lukelab.timeseries.TimeSeriesAnalysis" --master local[2] target/scala-2.11/timeseriesanalysis.jar --dataFile data/tsdata.csv --tenantId abc --stlEnabled true

Arguments:

--dataFile: path to the time series input file
--tenantId: tenant id for multi-tenancy support
--dbUrl: Url for mongodb. e.g. mongodb://localhost:27017/timeseries
--stlEnabled: define if we are going to use seasonality or not

