package com.goden;

import org.bson.Document;

import com.goden.mongo.MongoDBException;
import com.goden.mongo.MongoMgr;
import com.mongodb.BasicDBObject;

public class Context {
	
	public static final String dbName = "mydb";
	public static final String username = "goden";
	public static final String password = "411364";
	public static final String collectionName = dbName + ".test1";
	
	public static final String host = "127.0.0.1";
	public static final int port = 27017;

	public static void main(String[] argv) {
		
		BasicDBObject o1 = new BasicDBObject("aty", 1000);
		BasicDBObject o = new BasicDBObject("$set", o1);
		
		System.out.println(o.toJson());
		
		
		MongoMgr mgr;
		try {
			mgr = new MongoMgr(host, port, dbName);
			
			mgr.open(collectionName);
			
			//
			// list all product details
//			mgr.queryAll();
			
			//	Use below condition to filter out product whose size is M and price is greater than 50.
			//  {
			//		size: "M",
			//		price: {
			//			$gt: 50
			//		}
			//  }
			BasicDBObject filter = new BasicDBObject("size", "M");
			filter.append("price", new BasicDBObject("$gt", 50));
			
//			System.out.println(filter.toJson());
			
			mgr.query(filter, -1, true, true);
			
			//
			// insert new product
			Document document = new Document();
			document.put("productId", "I001");
			document.put("productName", "Milk Ice Cream");
			document.put("price", 100);
			
			mgr.insert(document);
			
//			mgr.queryAll();
			
			//
			// update price to 120
			// {
			//		$set: {
			//			price: 120
			//		}
			// }
			BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("price", 120));
			mgr.updateAll(new BasicDBObject("price", 100), update);
			mgr.queryAll();
			
			//
			// remove operation
			mgr.removeAll(new BasicDBObject("price", 120));
			mgr.queryAll();
			
			
			mgr.close();
			
		} catch (MongoDBException e) {
			e.printStackTrace();
		}
		
	}
	
}
