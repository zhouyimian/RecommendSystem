package com.km.offline

import breeze.numerics.sqrt
import com.km.scala.model.{MongoConfig, MovieRating}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.km.java.model.Constant._


object ALSTriner {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://RecommendSystem:27017/recommender",
      "mongo.db" -> "recommender"
    )


    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("ALSTriner").setMaster(config("spark.cores"))
    //创建一个Sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._
    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)).cache()

    //输出最优参数
    adjustASLParams(ratingRDD)

    spark.close()
  }
  //输出最终的最优参数
  def adjustASLParams(trainData:RDD[Rating]):Unit ={
    val result =for (rank <- Array(30,40,50,60,70); lambda <- Array(1,0.1,0.01))
      yield {
        val model = ALS.train(trainData,rank,5,lambda)
        val rmse = getRmse(model,trainData)
        (rank,lambda,rmse)
      }
    println(result.sortBy(_._3).head)
  }
  def getRmse(model:MatrixFactorizationModel,trainData:RDD[Rating]):Double ={
    val userMovies = trainData.map(item => (item.user,item.product))

    val predictRating = model.predict(userMovies)

    val real = trainData.map(item => ((item.user,item.product),item.rating))

    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

    sqrt(
      real.join(predict).map{
        case ((uid,mid),(real,pre)) =>
          val err = real-pre
          err*err
      }.mean()   //求均值
    )
  }
}
