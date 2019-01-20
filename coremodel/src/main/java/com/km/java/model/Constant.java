package com.km.java.model;

//定义整个业务系统的常量
public class Constant {
    //************** FOR MONGODB ****************

    public static String MONGODB_DATABASE = "recommender";

    public static String MONGODB_MOVIE_COLLECTION = "Movie";

    public static String MONGODB_RATING_COLLECTION = "Rating";

    public static String MONGODB_TAG_COLLECTION = "Tag";

    public static String MONGODB_USER_COLLECTION= "User";

    public static String MONGODB_AVERAGE_MOVIES = "AverageMovies";

    public static String MONGODB_GENRES_TOP_MOVIES = "GenresTopMovies";

    public static String MONGODB_RATE_MORE_MOVIES = "RateMoreMovies";

    public static String MONGODB_RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies";


    public static String MONGODB_STREAM_RECS_COLLECTION = "StreamRecs";

    public static String MONGODB_USER_RECS_COLLECTION = "UserRecs";

    public static String MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs";


    //************** FOR ELEASTICSEARCH ****************

    public static String ES_INDEX = "recommender";

    public static String ES_TYPE = "Movie";


    //************** Redis ******************
    public static int USER_RATING_QUEUE_SIZE=20;

    //************** LOG ******************
    public static String USER_RATING_LOG_PREFIX="USER_RATING_LOG_PREFIX";

    //**************Mongo Driver Class*************************
    public static String MONGO_DRIVER_CLASS = "com.mongodb.spark.sql";

    //**************ES Driver Class*************************
    public static String ES_DRIVER_CLASS = "org.elasticsearch.spark.sql";
}
