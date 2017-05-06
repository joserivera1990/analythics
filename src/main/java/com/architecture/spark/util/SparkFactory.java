package com.architecture.spark.util;

import com.mongodb.MongoClient;
import com.mongodb.util.JSONParseException;
import javaslang.control.Try;
import org.apache.spark.sql.*;

import java.nio.channels.UnresolvedAddressException;

public enum SparkFactory {

    CONNECTION;
    private SparkSession session = null;

     SparkFactory() {
        session = Try.of((()-> SparkSession.builder()
                          .appName("Spark shell")
                          .config("spark.driver.host", "127.0.0.1")
                          .getOrCreate()))
                     .getOrElseThrow((e) -> new UnresolvedAddressException());
    }

    public SparkSession getSession() {
        return session;
    }
}