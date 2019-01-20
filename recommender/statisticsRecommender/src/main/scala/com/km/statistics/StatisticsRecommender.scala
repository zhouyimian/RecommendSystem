package com.km.statistics


import java.text.SimpleDateFormat
import java.util.Date

import com.km.scala.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.km.java.model.Constant._



object StatisticsRecommender {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://RecommendSystem:27017/recommender",
      "mongo.db" -> "recommender"
    )


    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    //创建一个Sparksession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._
    //将数据集加载进来

    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .toDF()

    val movieDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()


    ratingDF.createOrReplaceTempView("ratings")
    //统计所有历史数据中每个电影的评分数
    //数据结构 -》mid count
    val rateMoreMoviesDF = spark.sql("select mid,count(mid) as count from ratings group by mid")

    rateMoreMoviesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATE_MORE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    //统计以月为单位每个电影的评分数
    //数据结构 -》mid count time

    //创建日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于将timestamp转换成年月格式
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x*1000L)).toInt)

    //将原来rating数据集中的时间转换成年月格式
    val ratingOfYearMonth = spark.sql("select mid,score,changeDate(timestamp) as yearmonth from ratings")

    //将新的数据集转换成一张表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMovies = spark.sql("select mid,count(mid) as count , yearmonth from ratingOfMonth group by yearmonth,mid")

    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()


    //统计每个电影的平均评分
    val averageMovies = spark.sql("select mid,avg(score) as avg from ratings group by mid")

    averageMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_AVERAGE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()
    //每种电影类别中评分最高的十个电影
    val movieWithScore = movieDF.join(averageMovies,Seq("mid","mid"))

    //所有电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    //将电影类别转换成RDD
    val genersRDD = spark.sparkContext.makeRDD(genres)
    //计算电影类别Top10
    val genrenTopMovies=genersRDD.cartesian(movieWithScore.rdd).filter{
      case (genres,row) => row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase())
    }.map{
      case (genres,row) =>{
        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
      }
    }.groupByKey()
      .map{
        case (genres,items) => GenresRecommendation(genres,items.toList.sortWith(_._2>_._2).take(10).map(item =>Recommendation(item._1,item._2)))
      }.toDF()

    genrenTopMovies
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    //关闭spark
    spark.stop()

  }
}
