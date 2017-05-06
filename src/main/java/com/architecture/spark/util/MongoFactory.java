package com.architecture.spark.util;

import com.mongodb.MongoClient;
import javaslang.control.Try;


public enum MongoFactory {

     CONNECTION;
     private Try<MongoClient> client = null;

     MongoFactory() {
         client = Try.of((()-> new MongoClient("localhost", 27017)));
     }

     public Try<MongoClient> getClient() {
         return client;
     }
}

