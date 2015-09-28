package com.lukelab.io

import java.util.Date
import com.mongodb._
import org.apache.spark.Logging

/**
 * Created by liyu on 9/26/15.
 */
class MongoDataSink (dbUrl: String, tenantId: String, dbName: String, collection: String) extends TimeSeriesDataSink with Logging {
  private val mongoClient: Option[MongoClient] = Some(new MongoClient(new MongoClientURI(dbUrl)))

  protected def writeToDB(client: MongoClient, jsonObject: BasicDBObject): Unit = {
      val database: DB = client.getDB(dbName)

      val appUsageCollection: DBCollection = database.getCollection(collection)
      appUsageCollection.insert(jsonObject, WriteConcern.UNACKNOWLEDGED)
  }

  override def save(data: Array[(Date, Double)]): Unit = {
    mongoClient match {
      case Some(client) =>
        data.foreach { case (ts, occurrence) =>
          val dbo = new BasicDBObject
          dbo.put("timestamp", ts)
          dbo.put("occurrence", occurrence.toInt)
          dbo.put("tenantid", tenantId)
          writeToDB(client, dbo)
        }
      case None =>
        log.error("mongo client is not created.")
    }

  }
}
