package com.lukelab.timeseries

/**
 * Created by liyu on 9/25/15.
 */
import java.util.Date

case class TimeOccurrence(ts: Date, occurrence: Double) {
  override def toString: String = {
    "timestamp: %s, occurrence %s".format(ts, occurrence)
  }
}
