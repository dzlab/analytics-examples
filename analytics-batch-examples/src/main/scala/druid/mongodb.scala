import com.mongodb.casbah.Imports._
import java.io.ByteArrayInputStream
import java.nio.file._
import sys.process._


object HelloWorld {
    def main(args: Array[String]) {
        val name = "sample"
        val columns = List("", "Murder", "Assault", "UrbanPop", "Rape")

        val contentString = "Arkansas,8.8,190,50,19.5\nCalifornia,9,276,91,40.6\nConnecticut,3.3,110,77,11.1\n"
        val content = new ByteArrayInputStream(contentString.getBytes)
        
        val path = Paths.get(s"/tmp/$name")
        Files.delete(path)
        Files.copy(content, path)
        s"mongoimport -f ${columns.reduce( _ +','+ _)} --type=csv /tmp/${name}" !
    }
}

/*
val mongoClient = MongoClient("localhost", 27017)

// get database and collections
val db = mongoClient("test")
db.collectionNames

// get a collection to work with
val coll = db("test")
// init bulk
//val builder = coll.initializeUnorderedBulkOperation
val buyerId = "azerty"
val publisherId = "4321"
val siteId = "1234"
val bulk = coll.initializeUnorderedBulkOperation
val a = MongoDBObject("revenue" -> 12, "_id" -> s"${siteId}:${publisherId}:${buyerId}")
bulk.insert(a)
bulk.execute
*/

