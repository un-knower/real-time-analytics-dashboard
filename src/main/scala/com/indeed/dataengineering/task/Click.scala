package com.indeed.dataengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */

import java.sql.Timestamp

case class ClickRawData(timestamp: Timestamp, country: String, city: String)
case class Click(timestamp: Timestamp, country: String, city: String, month: String, day: String, year: Int, hour: Int, week: Int, day_of_year: Int, day_of_month: Int)
case class LiveClickCount(timestamp: Timestamp, click_count: Int)