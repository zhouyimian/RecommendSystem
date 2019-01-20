package com.km.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.km.java.model.Constant;
import com.km.model.User;
import com.km.request.LoginUserRequest;
import com.km.request.RegisterUserRequest;
import com.km.request.UpdateUserGenresRequest;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class UserService {

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private ObjectMapper objectMapper;

    private MongoCollection<Document> userCollection;

    private MongoCollection<Document> getUserCollection(){
        if(null == userCollection)
            userCollection=mongoClient.getDatabase(Constant.MONGODB_DATABASE).getCollection(Constant.MONGODB_USER_COLLECTION);
        return userCollection;
    }

    private Document userToDocument(User user){
        try {
            Document document=Document.parse(objectMapper.writeValueAsString(user));
            return document;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private User DocumentToUser(Document document){
        try {
            User user = objectMapper.readValue(JSON.serialize(document),User.class);
            return user;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean registerUser(RegisterUserRequest request){
        if(getUserCollection().find(new Document("username",request.getUsername())).first()!=null)
            return false;

        User user=new User();
        user.setUsername(request.getUsername());
        user.setPassword(request.getPassword());
        user.setFirst(true);

        Document document=userToDocument(user);
        if(null==document)
            return false;
        getUserCollection().insertOne(document);
        return true;
    }

    public boolean loginUser(LoginUserRequest request){
        Document document = getUserCollection().find(new Document("username",request.getUsername())).first();
        if(null == document)
            return false;
        User user = DocumentToUser(document);
        if(null == user||user.getPassword().compareTo(request.getPassword())!=0)
            return false;
        return true;
    }

    public void updateUserGenres(UpdateUserGenresRequest request){
        getUserCollection().updateOne(new Document("username",request.getUsername()),new Document().append("$set",new Document("$genres",request.getGenres())));
        getUserCollection().updateOne(new Document("username",request.getUsername()),new Document().append("$set",new Document("$first",false)));
    }
    //通过用户名查询用户
    public User findUserByUsername(String username){
        Document document = getUserCollection().find(new Document("username",username)).first();
        if(null == document||document.isEmpty())
            return null;
        return DocumentToUser(document);
    }
}
