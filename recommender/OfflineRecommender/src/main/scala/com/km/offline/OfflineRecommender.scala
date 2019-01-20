package com.km.offline
import com.km.scala.model._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import com.km.java.model.Constant._


object OfflineRecommender {

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
      .format(MONGO_DRIVER_CLASS)
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
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()
    //计算用户推荐矩阵

    //需要构造一个userProducts RDD[(Int,Int)]
    val userMovies = userRDD.cartesian(movieRDD)

    val preRatings = model.predict(userMovies)

    val userRecs = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_USER_RECS_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

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
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    spark.close()
  }

  //余弦相似度计算
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
