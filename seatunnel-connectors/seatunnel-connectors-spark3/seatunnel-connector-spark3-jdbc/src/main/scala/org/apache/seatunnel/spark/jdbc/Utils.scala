package org.apache.seatunnel.spark.jdbc

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{toJavaDate, toJavaTimestamp}

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Locale, TimeZone}

object Utils {


  private val threadLocalDateFormat = new ThreadLocal[DateFormat] {

    override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    }
  }

  private val threadLocalTimestampFormat = new ThreadLocal[DateFormat] {
    override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    }
  }

  private def defaultTimeZone() = TimeZone.getDefault

  def getThreadLocalDateFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalDateFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  def dateToString(days: Int): String =
    getThreadLocalDateFormat(defaultTimeZone()).format(toJavaDate(days))

  def dateToString(days: Int, timeZone: TimeZone): String =
    getThreadLocalDateFormat(timeZone).format(toJavaDate(days))

  def timestampToString(us: Long): String = {
    timestampToString(us, defaultTimeZone())
  }

  def timestampToString(us: Long, timeZone: TimeZone): String = {
    val ts = toJavaTimestamp(us)
    val timestampString = ts.toString
    val timestampFormat = getThreadLocalTimestampFormat(timeZone)
    val formatted = timestampFormat.format(ts)

    if (timestampString.length > 19 && timestampString.substring(19) != ".0") {
      formatted + timestampString.substring(19)
    } else {
      formatted
    }
  }

  def getThreadLocalTimestampFormat(timeZone: TimeZone): DateFormat = {
    val sdf = threadLocalTimestampFormat.get()
    sdf.setTimeZone(timeZone)
    sdf
  }

  val DEFAULT_MAX_TO_STRING_FIELDS = 25

  def maxNumToStringFields = {
    if (SparkEnv.get != null) {
      SparkEnv.get.conf.getInt("spark.debug.maxToStringFields", DEFAULT_MAX_TO_STRING_FIELDS)
    } else {
      DEFAULT_MAX_TO_STRING_FIELDS
    }
  }
}
