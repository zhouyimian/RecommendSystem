package com.km.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.km.java.model.Constant._

import scala.collection.JavaConversions._


object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("RecommendSystem")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://RecommendSystem:27017/recommender"))
}
//推荐
case class Recommendation(rid: Int, r: Double)

//用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

case class MongConfig(uri:String,db:String)
object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://RecommendSystem:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )


    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    //创建一个Sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongoConfig = MongConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //广播电影相似度矩阵

    //转换为Map[Int,Map[Int,Double]]
    val simMovieMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_MOVIE_RECS_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRecs]
      .rdd
      .map{recs =>
        (recs.mid,recs.recs.map(x=> (x.rid,x.r)).toMap)
      }.collectAsMap()

    val simMoviesMatrixBroadCast = sc.broadcast(simMovieMatrix)

    val test = sc.makeRDD(1 to 2)
    test.map(x=>simMoviesMatrixBroadCast.value.get(1)).count()


    //创建kafka的连接
    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.43.44:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParams))

    //UID/MID/SCORE/TIMESTAMP
    //产生评分流
    val ratingStream = kafkaStream.map { case msg =>
      var attr = msg.value().split(",")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (uid, mid, score, timestamp) =>
        //获取当前最近的M次电影评分
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)
        //获取电影P最相似的K部电影
        val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)
        //计算待选电影的优先级
        val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)
        //将数据保存到Mongodb
        saveRecsToMongoDB(uid,streamRecs)
      }.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  //获取当前最近的M次电影评分
  /**
    *
    * @param num  评分的个数
    * @param uid  哪个用户
    * @return
    */
  def getUserRecentlyRating(num:Int,uid:Int,jedis:Jedis) : Array[(Int,Double)] = {
    jedis.lrange("uid:"+uid.toString,0,num).map{item=>
      val attr = item.split("\\:")
      (attr(0).trim.toInt,(attr(1).trim.toDouble))
    }.toArray
  }

  /**
    *
    * @param num              相似电影的数量
    * @param mid              当前电影的ID
    * @param uid              当前用户的ID
    * @param simMovies       电影相似度矩阵的广播变量值
    * @param mongConfig     Mongodb的配置
    * @return
    */
  //获取电影P最相似的K部电影
  def getTopSimMovies(num:Int,mid:Int,uid:Int,simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]])(implicit mongConfig:MongConfig): Array[Int] ={
    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray
    //获取用户已经看过的电影
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map{item =>
      item.get("mid").toString.toInt
    }
    //过滤掉已经评分过的电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2>_._2).take(num).map(x => x._1)
  }

  /**
    * 计算待选电影的优先级
    * @param simMovies                 电影相似度矩阵
    * @param userRecentlyRatings      用户最近的k次评分
    * @param topSimMovies              当前电影最相似的k个电影
    * @return
    */
  def computeMovieScores(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRecentlyRatings: Array[(Int,Double)],topSimMovies:Array[Int]): Array[(Int,Double)] = {

    //用于保存每个待选电影和最近评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()

    //用于保存每个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()

    //用于保存每个电影的减弱因子数
    val decreMap =scala.collection.mutable.HashMap[Int,Int]()
    for(topSimMovie <- topSimMovies;userRecentlyRating <- userRecentlyRatings){
      val simScore = getMovieSimScore(simMovies,userRecentlyRating._1,topSimMovie)
      if(simScore>0.6){
        score+=((topSimMovie,simScore*userRecentlyRating._2))
        if(userRecentlyRating._2>3){
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0)+1
        }else{
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie,0)+1
        }
      }
    }
    score.groupBy(_._1).map{case (mid,sims)=>
      (mid,sims.map(_._2).sum / sims.length + log(increMap(mid)) -log(decreMap(mid)))
    }.toArray
  }

  def log(m:Int):Double={
    math.log(m)/math.log(2)
  }

  /**
    * 获取两个电影之间的相似度
    * @param simMovies          电影相似度矩阵
    * @param userRatingMovie    用户已经评分的电影
    * @param topSimMovie        候选电影
    * @return
    */
  def getMovieSimScore(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRatingMovie:Int,topSimMovie:Int): Double = {
    simMovies.get(topSimMovie) match {
      case Some(sim) => sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None =>0.0
    }
  }

  /**
    * 将数据保存到Mongodb  uid -> 1,recs -> 22:4.5|45,3.3
    * @param uid  流式的推荐结果
    * @param streamRecs  流式的推荐结果
    * @param mongConfig  Mongodb的配置
    */
  def saveRecsToMongoDB(uid:Int,streamRecs:Array[(Int,Double)])(implicit mongConfig: MongConfig):Unit= {
    //到StreamRecs的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))

    streamRecsCollection.insert(MongoDBObject("uid" -> uid,"recs" -> streamRecs.map(x=> x._1+":"+x._2).mkString("|")))
  }
}
