package com.km.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

case class MongoConfig(val uri: String, val db: String)

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

//推荐
case class Recommendation(rid: Int, r: Double)

//用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val USER_RECS = "Userrecs"
  val MOVIE_RECE= "Movierecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://RecommendSystem:27017/recommender",
      "mongo.db" -> "recommender"
    )


    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
      .set("spark.executor.memory", "3G").set("spark.driver.memory", "1G")
    //创建一个Sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //读取mongodb的业务数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()

    //创建训练数据集
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterators, lambda) = (50, 5, 0.01)
    //训练ALS模型
    val model = ALS.train(trainData, rank, iterators, lambda)

    val userRDD = ratingRDD.map(_._1).distinct()

    val movieRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()
    //计算用户推荐矩阵

    //需要构造一个userProducts RDD[(Int,Int)]
//    val userMovies = userRDD.cartesian(movieRDD)
//
//    val preRatings = model.predict(userMovies)
//
//    val userRecs = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
//      .groupByKey()
//      .map {
//        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
//      }.toDF()
//
//    userRecs.write
//      .option("uri", mongoConfig.uri)
//      .option("collection", USER_RECS)
//      .mode("overwrite")
//      .format("com.mongodb.spark.sql")
//      .save()

    //计算电影相似度矩阵
    //获取电影的特征矩阵
    val movieFeatures = model.productFeatures.map {
      case (mid, features) =>
        (mid, new DoubleMatrix(features))
    }
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 }
      .map {
        case (a, b) =>
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map{
        case (mid,items) =>
          MovieRecs(mid,items.toList.map(x => Recommendation(x._1,x._2)))
      }.toDF()

    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.close()
  }

  //余弦相似度计算
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
