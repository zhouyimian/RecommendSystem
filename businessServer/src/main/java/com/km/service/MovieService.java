package com.km.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.km.java.model.Constant;
import com.km.model.Movie;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class MovieService {

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private ObjectMapper objectMapper;

    private MongoCollection<Document> movieCollection;

    private MongoCollection<Document> getMovieCollection() {
        if(movieCollection==null)
            this.mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_MOVIE_COLLECTION);
        return this.movieCollection;
    }

    private Movie documentToMovie(Document document){
        try {
            Movie movie = objectMapper.readValue(JSON.serialize(document),Movie.class);
            Document score = mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_AVERAGE_MOVIES).find(Filters.eq("mid",movie.getMid())).first();
            if(score == null ||score.isEmpty())
                movie.setScore(0D);
            else
                movie.setScore(score.getDouble("avg"));
            return movie;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Document movieToDocument(Movie movie){
        try {
            Document document = Document.parse(objectMapper.writeValueAsString(movie));
            return document;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Movie> getMoviesByMids(List<Integer> ids){
        List<Movie> result = new ArrayList<>();
        FindIterable<Document> documents = getMovieCollection().find(Filters.in("mid",ids));
        for(Document document:documents){
            result.add(documentToMovie(document));
        }
        return result;
    }

    public Movie findMovieInfo(int mid){
        Document document = getMovieCollection().find(new Document("mid",mid)).first();
        if(null == document ||document.isEmpty())
            return null;
        return documentToMovie(document);
    }
}
