package com.km.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.km.java.model.Constant;
import com.km.model.Rating;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.io.IOException;

@Service
public class RatingService {

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Jedis jedis;

    private MongoCollection<Document> RatingCollection;

    private MongoCollection<Document> getRatingCollection() {
        if(RatingCollection==null)
            this.mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_RATING_COLLECTION);
        return this.RatingCollection;
    }

    private Document ratingToDocument(Rating rating){
        try {
            Document document = Document.parse(objectMapper.writeValueAsString(rating));
            return document;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Rating documentToRating(Document document){
        try {
            Rating rating = objectMapper.readValue(JSON.serialize(document),Rating.class);
            return rating;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void rateToMovie(Rating rating){
        getRatingCollection().insertOne(ratingToDocument(rating));
        //更新redis
        updateRedis(rating);
    }

    private void updateRedis(Rating rating){
        if(jedis.llen("uid:"+rating.getUid()) >= Constant.USER_RATING_QUEUE_SIZE){
            jedis.rpop("uid:"+rating.getUid());
        }
        jedis.lpush("uid:"+rating.getUid(), rating.getMid()+":"+rating.getScore());
    }

}
