package com.km.service;

import com.km.java.model.Constant;
import com.km.model.Recommendation;
import com.km.request.*;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class RecommenderService {

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private TransportClient esClient;

    private MongoDatabase mongoDatabase;

    private MongoDatabase getMongoDatabase(){
        if(mongoDatabase==null)
            this.mongoDatabase = mongoClient.getDatabase(Constant.MONGODB_DATABASE);
        return this.mongoDatabase;
    }


    public List<Recommendation> getHybridRecommendations(GetHybridRecommendationRequest request) {
        //获取电影相似矩阵的结果
        List<Recommendation> itemCF = getItemCFMovies(new GetItemCFMoviesRequest(request.getMid(),request.getNum()));

        //基于内容推荐的推荐结果
        List<Recommendation> contentBased = getContentBasedRecommendations
                (new GetContentBasedRecommendationRequest(request.getMid(),request.getNum()));

        //返回结果
        List<Recommendation> result = new ArrayList<>();
        result.addAll(itemCF.subList(0,(int)Math.round(itemCF.size()*request.getCfShare())));
        result.addAll(contentBased.subList(0,(int)Math.round(contentBased.size()*(1-request.getCfShare()))));
        return result;
    }

    //获取当前用户的实时推荐
    public List<Recommendation> getStreamRecsMovies(GetStreamRecsRequest request) {
        MongoCollection<Document> streamRecsCollection = getMongoDatabase().getCollection(Constant.MONGODB_STREAM_RECS_COLLECTION);
        Document document = streamRecsCollection.find(new Document("uid", request.getUid())).first();
        List<Recommendation> result = new ArrayList<>();
        if (null == document || document.isEmpty())
            return result;
        for (String item : document.getString("recs").split("\\|")) {
            String[] para = item.split(":");
            result.add(new Recommendation(Integer.parseInt(para[0]), Double.parseDouble(para[1])));
        }
        return result.subList(0, result.size() > request.getNum() ? request.getNum() : result.size());
    }

    //ALS算法中用户推荐矩阵
    public List<Recommendation> getUserCFMovies(GetUserCFRequest request) {
        MongoCollection<Document> userCFCollection = getMongoDatabase().getCollection(Constant.MONGODB_USER_RECS_COLLECTION);
        Document document = userCFCollection.find(new Document("uid", request.getUid())).first();
        return parseDocument(document, request.getSum());
    }

    //
    public List<Recommendation> getItemCFMovies(GetItemCFMoviesRequest request) {
        MongoCollection<Document> itemCFCollection = getMongoDatabase().getCollection(Constant.MONGODB_MOVIE_RECS_COLLECTION);
        Document document = itemCFCollection.find(new Document("mid", request.getMid())).first();
        return parseDocument(document, request.getNum());
    }


    public List<Recommendation> parseDocument(Document document, int sum) {
        List<Recommendation> result = new ArrayList<>();
        if (null == document || document.isEmpty())
            return result;
        ArrayList<Document> documents = document.get("recs", ArrayList.class);
        for (Document item : documents) {
            result.add(new Recommendation(item.getInteger("rid"), item.getDouble("r")));
        }
        return result.subList(0, result.size() > sum ? sum : result.size());
    }

    //基于内容的推荐
    public List<Recommendation> getContentBasedRecommendations(GetContentBasedRecommendationRequest request) {
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery
                (new MoreLikeThisQueryBuilder.Item[]{new MoreLikeThisQueryBuilder.Item
                        (Constant.ES_INDEX, Constant.ES_TYPE, String.valueOf(request.getMid()))
                });
        SearchResponse response = esClient.prepareSearch(Constant.ES_INDEX).setQuery(queryBuilder).setSize(request.getNum()).execute().actionGet();
        return parseESResponse(response);
    }

    public List<Recommendation> parseESResponse(SearchResponse response) {
        List<Recommendation> recommendations = new ArrayList<>();
        for(SearchHit hit:response.getHits()){
            Map<String,Object> hitcontent = hit.getSourceAsMap();
            recommendations.add(new Recommendation((int)(hitcontent.get("mid")),0D));
        }
        return recommendations;
    }

    /**
     *获取电影类别的Top电影，解决冷启动问题
     * @param request
     * @return
     */
    public List<Recommendation> getGenresTopMovies(GetGenresTopMovieRequest request){
        Document genresDocument = getMongoDatabase()
                .getCollection(Constant.MONGODB_GENRES_TOP_MOVIES)
                .find(new Document("genres",request.getGenres())).first();
        List<Recommendation> recommendations = new ArrayList<>();
        if(null == genresDocument||genresDocument.isEmpty())
            return recommendations;
        return parseDocument(genresDocument,request.getNum());
    }

    /**
     * 获取热门电影
     * @param request
     * @return
     */
    public List<Recommendation> getHotRecommendations(GetHotRecommendationRequest request){
        FindIterable<Document> documents = getMongoDatabase().getCollection(Constant.MONGODB_RATE_MORE_RECENTLY_MOVIES).find().sort(Sorts.descending("yearmouth"));
        List<Recommendation> recommendations = new ArrayList<>();
        for(Document item:documents){
            recommendations.add(new Recommendation(item.getInteger("mid"),0D));
        }
        return recommendations.subList(0,recommendations.size()>request.getNum()?request.getNum():recommendations.size());
    }

    /**
     * 获取优质电影
     * @param request
     * @return
     */
    public List<Recommendation> getRateModeMovies(GetRateMoreMoviesRequest request){
        FindIterable<Document> documents = getMongoDatabase().getCollection(Constant.MONGODB_RATE_MORE_MOVIES).find().sort(Sorts.descending("count"));
        List<Recommendation> recommendations = new ArrayList<>();
        for(Document item:documents){
            recommendations.add(new Recommendation(item.getInteger("mid"),0D));
        }
        return recommendations.subList(0,recommendations.size()>request.getNum()?request.getNum():recommendations.size());
    }
    /**
     * 获取最新电影
     * @param request
     * @return
     */
    public List<Recommendation> getNewMovies(GetNewMoviesRequest request){
        FindIterable<Document> documents = getMongoDatabase().getCollection(Constant.MONGODB_MOVIE_COLLECTION).find().sort(Sorts.descending("issue"));
        List<Recommendation> recommendations = new ArrayList<>();
        for(Document item:documents){
            recommendations.add(new Recommendation(item.getInteger("mid"),0D));
        }
        return recommendations.subList(0,recommendations.size()>request.getNum()?request.getNum():recommendations.size());
    }

    /**
     * 模糊主题检索
     * @param request
     * @return
     */
    public List<Recommendation> getFuzzMovies(GetFuzzySearchMoviesRequest request){
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("name",request.getQuery());
        SearchResponse response = esClient.prepareSearch(Constant.ES_INDEX).setQuery(queryBuilder).setSize(request.getNum()).execute().actionGet();
        return parseESResponse(response);
    }

    public List<Recommendation> getGenresMovies(GetGenresMovieRequest request){
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("genres",request.getGenres());
        SearchResponse response = esClient.prepareSearch(Constant.ES_INDEX).setQuery(queryBuilder).setSize(request.getNum()).execute().actionGet();
        return parseESResponse(response);
    }
}
