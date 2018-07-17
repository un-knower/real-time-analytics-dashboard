package com.indeed.dataengineering.task

import java.sql.Timestamp

case class ClickAnalytics(timestamp: Timestamp, country: String, city: String)