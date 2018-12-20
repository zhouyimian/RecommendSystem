package com.km.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import sun.net.InetAddressCachePolicy

//movie数据集通过^ 分隔符
//151^                                 电影ID
// Rob Roy (1995)^                     电影名称
// In the highlands of Scotlan         电影描述
// 139 minutes^                        电影时长
// August 26, 1997^                    电影发行时期
// 1995^                               电影拍摄日期
// English ^                           电影语言
// Action|Drama|Romance|War ^          电影类型
// Liam Neeson|Jess|m Neeson           电影演员
// Caton-Jones                         电影导演


//Ratings数据集 用户对电影的评分
//1,                     用户ID
// 31,                   电影ID
// 2.5,                  用户对电影的评分
// 1260759144            用户对电影评分的时间


//tags数据集，用户对电影的标签数据集
//15,                     用户ID
// 339,                   电影ID
// sandra 'boring' bu     标签的具体内容
// 1138537770             用户对电影打标签的时间

//数据的主加载服务

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  *MongoDB的连接配置
  * @param uri MongoDB的连接
  * @param db  MongoDB要操作的数据库
  */
case class MongoConfig(val uri:String,val db:String)

/**
  * elasticSearch的连接配置
  * @param HttpHosts        HTTP的主机列表，以逗号分割
  * @param transportHosts  Transport主机列表，用逗号分隔
  * @param index            需要操作的索引
  * @param clustername      ES集群的名称
  */
case class ESconfig(val HttpHosts:String,val transportHosts:String,val index:String,val clustername:String)

object Dataloader {
  val MOVIE_DATA_PATH ="D:\\IDEA_project\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\movies.csv"
  val RATING_DATA_PATH ="D:\\IDEA_project\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\ratings.csv"
  val TAG_DATA_PATH ="D:\\IDEA_project\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\tags.csv"

  val MONGODN_MOVIE_COLLECTION="Movie"
  val MONGODN_RATING_COLLECTION="Rating"
  val MONGODN_TAG_COLLECTION="Tag"

  val ES_MOVIE_INDEX="Movie"
  val ES_RATING_INDEX="Rating"
  val ES_TAG_INDEX="Tag"
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://RecommendSystem:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "RecommendSystem:9200",
      "es.transportHosts" -> "RecommendSystem:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)
    //创建一个Sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    //将三个数据集加载进来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    //将movieRDD转换为DataFrame
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).trim.toInt, attr(1).trim, attr(2).trim, attr(3).trim,
        attr(4).trim, attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()


    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将ratingRDD转换为DataFrame
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()


    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    //将tagRDD转换为DataFrame
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).toString, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)

    //将数据保存到mongodb
    //storeDataInMongoDB(movieDF,ratingDF,tagDF)


    //新的movie数据集(额外增加了tag字段)
    //151^                                 电影ID
    // Rob Roy (1995)^                     电影名称
    // In the highlands of Scotlan         电影描述
    // 139 minutes^                        电影时长
    // August 26, 1997^                    电影发行时期
    // 1995^                               电影拍摄日期
    // English ^                           电影语言
    // Action|Drama|Romance|War ^          电影类型
    // Liam Neeson|Jess|m Neeson           电影演员
    // Caton-Jones                         电影导演
    // tag1/tag2/...                       电影的标签


    //这里对tag数据集进行处理，处理后的形式为  MID   tag1/tag2/...
    import org.apache.spark.sql.functions._
    /**
      * MID,Tags
      * 1   tag1/tag2/...
      */
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags"))
    //需要将处理后的tag数据和movie数据融合，产生新的movie数据
    val movieWithTagsDF = movieDF.join(newTag,Seq("mid","mid"),"left").select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")


    implicit val esConfig = ESconfig(config.get("es.httpHosts").get,config.get("es.transportHosts").get,
      config.get("es.index").get,config.get("es.cluster.name").get)
    //将数据保存到es
    storeDataInES(movieWithTagsDF)

    //关闭spark
    spark.stop()

  }

  //将数据保存到mongodb的方法
  def storeDataInMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建一个到mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中有对应的数据库，应该删除
    mongoClient(mongoConfig.db)(MONGODN_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODN_TAG_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODN_RATING_COLLECTION).dropCollection()

    //将当前数据写入mongodb
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODN_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODN_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    tagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODN_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据库表建立索引
    mongoClient(mongoConfig.db)(MONGODN_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODN_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODN_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODN_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODN_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))

    //关闭mongodb连接
    mongoClient.close()
  }

  //将数据保存到es的方法
  def storeDataInES(movieWithTagsDF:DataFrame)(implicit eSConfig: ESconfig): Unit = {
    //新建一个配置
    val settings:Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()

    //新建ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将transportHosts添加到esClient中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) =>{
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    //需要清除ES遗留的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))


    //将数据写入到ES中
    movieWithTagsDF
      .write
      .option("es.nodes",eSConfig.HttpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index+"/"+ES_MOVIE_INDEX)
  }
}
