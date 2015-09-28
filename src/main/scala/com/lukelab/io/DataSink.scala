package com.lukelab.io

import java.util.Date

/**
 * Created by liyu on 9/26/15.
 */
trait TimeSeriesDataSink {
  def save(data: Array[(Date, Double)])
}