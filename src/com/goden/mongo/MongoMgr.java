package com.goden.mongo;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoMgr {
	
	MongoCredential credential;
	MongoClient client;
	MongoDatabase db;
	MongoCollection<Document> collection;
	

	private boolean consoleOutput = true;
	

	public MongoMgr(String host, int port, String dbName) throws MongoDBException {
		
		client = new MongoClient(new MongoClientURI("mongodb://" + host + ":" + port + "/" + dbName));
		db = client.getDatabase(dbName);
		
		if (db == null) {
			throw new MongoDBException();
		}
		
	}
	
	public MongoMgr(String host, int port, String dbName, String username, String password) throws MongoDBException {
		
		credential = MongoCredential.createMongoCRCredential(username, dbName, password.toCharArray());
		client =  new MongoClient(new ServerAddress(host, port), Arrays.asList(credential));
		
		db = client.getDatabase(dbName);
		
		if (db == null) {
			throw new MongoDBException();
		}
		
	}
	
	public void open(String collectionName) throws MongoCollectionNotExistException {		
		collection = db.getCollection(collectionName);
		if (collection == null) {
			throw new MongoCollectionNotExistException();
		}
	}
	
	public void query(BasicDBObject filter, int limit, boolean ascending, boolean consoleOutput) {
		
		FindIterable<Document> i;
		if (filter != null) {
			i = collection.find(filter);
		} else {
			i = collection.find();
		}
		
		if (limit != -1) {
			i = i.limit(limit);
		}
		
		BasicDBObject sorting = new BasicDBObject("productId", ascending ? 1 : -1);
		i = i.sort(sorting);
		
		MongoCursor<Document> iterator = i.iterator();
		while(iterator.hasNext()) {

			Document d = iterator.next();
			
			System.out.println(d.toJson());
			
		}
		
	}
	
	
	public void queryAll() {
		query(null, -1, true, consoleOutput);
	}
	
	public void insert(Document document) {
		collection.insertOne(document);
	}
	
	public void insert(List<Document> documents) {
		collection.insertMany(documents);
	}
	
	public void remove(BasicDBObject filter, boolean bulkWritable) {
		if (bulkWritable) {
			collection.deleteMany(filter);
		} else {
			collection.deleteOne(filter);
		}
	}
	
	public void removeAll(BasicDBObject filter) {
		remove(filter, true);
	}
	
	public void update(BasicDBObject filter, BasicDBObject update, boolean bulkWritable) {
		if (bulkWritable) {
			collection.updateMany(filter, update);
		} else {
			collection.updateOne(filter, update);
		}
	}
	
	public void updateAll(BasicDBObject filter, BasicDBObject update) {
		update(filter, update, true);
	}
	
	public void close() {
		client.close();
	}
	
}