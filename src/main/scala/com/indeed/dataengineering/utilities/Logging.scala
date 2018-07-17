package com.indeed.dataengineering.utilities

/**
  * Created by aguyyala on 2/16/17.
  */

import org.apache.log4j.{Level, Logger}


trait Logging {

      val log: Logger = Logger.getLogger(getClass)
      log.setLevel(Level.toLevel("Info"))

}
