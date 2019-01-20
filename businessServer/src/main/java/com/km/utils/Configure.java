package com.km.utils;


import com.mongodb.MongoClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

@Configuration
public class Configure {

    private String mongoHost;
    private int mongoPort;
    private String esClusterName;
    private String esHost;
    private int esPort;
    private String redisHost;

    public Configure(){
        try{
            Properties properties = new Properties();
            Resource resource = new ClassPathResource("application.properties");
            properties.load(new FileInputStream(resource.getFile()));
            this.mongoHost = properties.getProperty("mongo.host");
            this.mongoPort = Integer.parseInt(properties.getProperty("mongo.port"));
            this.esClusterName = properties.getProperty("es.cluster.name");
            this.esHost = properties.getProperty("es.host");
            this.esPort = Integer.parseInt(properties.getProperty("es.port"));
            this.redisHost = properties.getProperty("redis.host");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    @Bean(name = "mongoClient")
    public MongoClient getMongoClient(){
        MongoClient mongoClient = new MongoClient( this.mongoHost , this.mongoPort );
        return mongoClient;
    }

    @Bean(name = "transportClient")
    public TransportClient getTransportClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name",this.esClusterName).build();
        TransportClient esClient = new PreBuiltTransportClient(settings);
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(this.esHost), this.esPort));
        return esClient;
    }

    @Bean(name = "jedis")
    public Jedis getRedisClient() {
        Jedis jedis = new Jedis(this.redisHost);
        return jedis;
    }
}
