import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import edu.stanford.nlp.pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
object GetStreamingTweetsTraffic {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load.getConfig(args(0))
    val spark = SparkSession.
      builder().
      appName("Get Streaming Tweets Traffic").
      master(conf.getString("execution.mode")).
      getOrCreate()
    val sqlContext = new SQLContext(spark.sparkContext)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val lines = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", conf.getString("bootstrap.servers")).
      option("subscribe", "twitter_tweets").
      option("includeTimestamp", true).
      load().
     selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp").
      as[(String, String, Timestamp)]
    //lines.printSchema();
    val schema = new StructType().add($"created_at".string).add($"tweet_id".string).add($"tweet".string).add($"user_id".string).
      add($"user_name".string).add($"followers_count".string).add($"friends_count".string).
      add($"hashtags".string).add($"sentiments".string).add($"place".string)
    val encoder = RowEncoder(schema)
    val df = lines.select(get_json_object($"value", "$.created_at").alias("created_at"),
             get_json_object($"value", "$.id_str").alias("ID"),
             get_json_object($"value", "$.text").alias("tweet"),
             get_json_object($"value", "$.extended_tweet.full_text").alias("long_tweet"),
             get_json_object($"value", "$.entities.hashtags").alias("hashtags"),
             get_json_object($"value", "$.user.id_str").alias("user_id"),
             get_json_object($"value", "$.user.name").alias("user_name"),
             get_json_object($"value", "$.user.followers_count").alias("followers_count"),
             get_json_object($"value", "$.user.friends_count").alias("friends_count"),
             get_json_object($"value", "$.user.lang").alias("lang"), get_json_object($"value", "$.sentiment").alias("sentiment"),
             get_json_object($"value", "$.place.full_name").alias("place"))
     val engTweets = df.filter(f => f.getAs[String]("lang") == "en")
    val sentiment = engTweets.map(s => {
      if(s.getAs[String]("long_tweet") == null) {
        val text = s.getAs[String]("tweet").split(" ").filter(_.matches("(^[a-zA-Z0-9 ]+$)")).fold("")((a, b) => a + " " + b).trim
        val sent = SentimentAnalysisUtils.detectSentiment(text)
        Row(s.getAs[String]("created_at"), s.getAs[String]("ID"), s.getAs[String]("tweet"),
          s.getAs[String]("user_id"),
          s.getAs[String]("user_name"), s.getAs[String]("followers_count"),
          s.getAs[String]("friends_count"), s.getAs[String]("hashtags"),
          sent.toString, s.getAs[String]("place"))
      }
     else{
        val text = s.getAs[String]("long_tweet").split(" ").filter(_.matches("(^[a-zA-Z0-9 ]+$)")).fold("")((a, b) => a + " " + b).trim
        val sent =SentimentAnalysisUtils.detectSentiment(text)
        Row(s.getAs[String]("created_at"), s.getAs[String]("ID"), s.getAs[String]("long_tweet"),
          s.getAs[String]("user_id"), s.getAs[String]("user_name"),
          s.getAs[String]("followers_count"), s.getAs[String]("friends_count"),
          s.getAs[String]("hashtags"), sent.toString, s.getAs[String]("place"))
      }

    })(encoder)
	
	System.out.println("Saving to HBase")

    //Foreach Writer to store in HBase
    val writer = new ForeachWriter[Row] {
      var hBaseConf: Configuration = _
      var connection: Connection = _
      override def open(partitionId: Long, version: Long) = {
        System.out.println("Starting to connect to HBase.......................")
        hBaseConf = HBaseConfiguration.create()
        hBaseConf.set("hbase.zookeeper.quorum", conf.getString("zookeeper.quorum"))
        hBaseConf.set("hbase.zookeeper.property.clientPort", conf.getString("zookeeper.port"))
        if(args(0) != "dev") {
          hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
          hBaseConf.set("hbase.cluster.distributed", "true")
        }
        connection = ConnectionFactory.createConnection(hBaseConf)
        System.out.println("Successfully Connected to HBase ..............")
        true
      }

      override def process(value: Row)={
        val table = connection.getTable(TableName.valueOf("twitter:twitter_data"))
        val rowKey = Bytes.toBytes(value.get(1).toString)
        val row = new Put(rowKey)
        row.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("created_at"), Bytes.toBytes(value.get(0).toString))
        row.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("text"), Bytes.toBytes(value.get(2).toString))
        row.addColumn(Bytes.toBytes("user"), Bytes.toBytes("user_id"), Bytes.toBytes(value.get(3).toString))
        row.addColumn(Bytes.toBytes("user"), Bytes.toBytes("user_name"), Bytes.toBytes(value.get(4).toString))
        row.addColumn(Bytes.toBytes("user"), Bytes.toBytes("followers_count"), Bytes.toBytes(value.get(5).toString))
        row.addColumn(Bytes.toBytes("user"), Bytes.toBytes("friends_count"), Bytes.toBytes(value.get(6).toString))
        row.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("hashtags"), Bytes.toBytes(value.get(7).toString))
        row.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("sentiment"), Bytes.toBytes(value.get(8).toString))
        if(value.get(9)!=null)
          row.addColumn(Bytes.toBytes("tweets"), Bytes.toBytes("place"), Bytes.toBytes(value.get(9).toString))
        table.put(row)
      }

      override def close(errorOrNull: Throwable)= {
        connection.close()
      }
    }
    if(args(1) == "HBase") {
      val query: StreamingQuery = sentiment.writeStream.
        foreach(writer).
        outputMode("append").
        trigger(Trigger.ProcessingTime("10 seconds")).
        start()
      query.awaitTermination()
    }
      //console output mode to test the application
    else{
      val query: StreamingQuery = sentiment.writeStream.
        format("console").
        outputMode("append").
        trigger(Trigger.ProcessingTime("10 seconds")).
        start()
      query.awaitTermination()

    }
  }
}
