package com.km.scala.model
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
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)
//Ratings数据集 用户对电影的评分
//1,                     用户ID
// 31,                   电影ID
// 2.5,                  用户对电影的评分
// 1260759144            用户对电影评分的时间
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)
//tags数据集，用户对电影的标签数据集
//15,                     用户ID
// 339,                   电影ID
// sandra 'boring' bu     标签的具体内容
// 1138537770             用户对电影打标签的时间
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


//推荐
case class Recommendation(rid: Int, r: Double)

//用户的推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影的相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])


/**
  * 电影类别推荐结果
  * @param genres    电影类别
  * @param recs       Top10的电影集合
  */
case class GenresRecommendation(genres:String,recs:Seq[Recommendation])

object Model {

}
