package util

/**
  * Created by arjunpuri on 1/8/17.
  */

object CSVUtil {
  /**
    * Wrapper to convert case classes into CSV
    * @param prod
    */
  implicit class CSVWrapper(val prod: Product) extends AnyVal {
    def toCSV(delim: String) = prod.productIterator.map{
      case Some(value) => value
      case None => ""
      case rest => rest
    }.mkString(delim)
  }
}

