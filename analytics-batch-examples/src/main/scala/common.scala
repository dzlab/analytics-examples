package common
import java.io._
import com.github.tototoshi.csv._
import Stream._

object tsv extends TSVFormat {}

object CSVUtils {

  def read(filename: String, isCSV: Boolean): CSVReader = {
    if(isCSV == false)  
      CSVReader.open(new File(filename))(tsv)
    else
      CSVReader.open(new File(filename))
  }

}
