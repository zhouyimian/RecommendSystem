package com.km.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.km.java.model.Constant;
import com.km.model.Tag;
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
public class TagService {


    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private ObjectMapper objectMapper;

    private Document TagToDocument(Tag tag){
        try {
            Document document=Document.parse(objectMapper.writeValueAsString(tag));
            return document;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private Tag DocumentToTag(Document document){
        try {
            Tag tag = objectMapper.readValue(JSON.serialize(document),Tag.class);
            return tag;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }



    private MongoCollection<Document> TagCollection;

    private MongoCollection<Document> getTagCollection() {
        if(TagCollection==null)
            this.mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_TAG_COLLECTION);
        return this.TagCollection;
    }

    public List<Tag> getMovieTags(int mid){
        FindIterable<Document> documents = getTagCollection().find(Filters.eq("mid",mid));
        List<Tag> tags = new ArrayList<>();
        for(Document item:documents){
            tags.add(DocumentToTag(item));
        }
        return tags;
    }

    public void addTagToMovie(Tag tag){
        getTagCollection().insertOne(TagToDocument(tag));
    }
}
