package com.architecture.spark.services.impl;

import com.mongodb.*;

import com.architecture.spark.services.Mongo;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import javaslang.control.Try;
import org.bson.Document;

public class MongoImpl implements Mongo {

	public void saveObject(Object object, String collection, Try<MongoClient> mongoClient)  {
		mongoClient.map(client -> client.getDatabase("CPP"))
				   .map(db -> db.getCollection(collection))
				   .andThenTry(col ->  col.insertOne(new Document(buildBasicDBObject(object))))
		           .onFailure(e -> System.out.println("Error save one register"+ e.getMessage()));
	}

    private BasicDBObject buildBasicDBObject(Object object) {
		return Try.of( () -> (BasicDBObject) JSON.parse(object.toString()))
				  .getOrElseThrow((e) -> new JSONParseException(e.getMessage(),0));
	}

	

	
}
