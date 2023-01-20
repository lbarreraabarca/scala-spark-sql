package com.data.factory

import com.data.factory.adapters.{Base64Encoder, SparkSessionFactory}
import com.data.factory.exceptions.RequestException
import com.data.factory.models.Payload
import com.data.factory.ports.Encoder
import com.typesafe.scalalogging.Logger
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

object App extends Serializable{
    val log = Logger("App")

    private def makeRequest(jsonInput: String): Payload = try {
        implicit val formats = Serialization.formats(NoTypeHints)
        read[Payload](jsonInput)
    }catch {
        case e: Exception => throw RequestException(e.getClass.toString.concat(":").concat(e.getMessage.toString))
    }

    def main(args: Array[String]): Unit = {
        val encodedInput = args(0)
        try {
            log.info("Creating SparkSession")
            val sparkSession = new SparkSessionFactory()
            log.info("Creating Spark Cluster")
            val spark = sparkSession.makeLocal()
            val encoder: Encoder = new Base64Encoder()
            log.info("Decoding encodedInput %s".format(encodedInput))
            val decodedInput = encoder.decode(encodedInput)
            log.info("Decoded input %s".format(decodedInput))
            val request = makeRequest(decodedInput)

            log.info("Creating RDD")
            val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
            val rdd = spark.sparkContext.parallelize(dataSeq)
            val df = spark.createDataFrame(rdd)
            log.info(rdd.count.toString)
            log.info(df.printSchema().toString)
            log.info("Process ended successfully.")
        } catch {
            case e: Exception => throw RequestException(e.getClass.toString.concat(":").concat(e.getMessage))
        }
    }
}
