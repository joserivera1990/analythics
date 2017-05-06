package com.architecture.spark.services;

import com.mongodb.MongoClient;
import javaslang.control.Try;

public interface Mongo {

	void saveObject(Object object, String collection, Try<MongoClient> mongo);
}
