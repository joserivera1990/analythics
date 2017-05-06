package com.architecture.spark.analythics;

import com.architecture.spark.util.SparkFactory;
import com.mongodb.hadoop.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


import static javaslang.Predicates.*;
import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;

public class HearBeatAnalythics {

    private static final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd:HH");
    private static final String SEPARATE_CHAR = ";";
    private static final String DATE_TYPE = "date";
    public static void main(String[] args) {

        JavaPairRDD<Object, BSONObject> documents = getDocumentsFromDB();
        SparkSession spark = SparkFactory.CONNECTION.getSession();
        StructType schema = getSchema();

        System.out.println("count-->" + documents.count() );

        JavaRDD<Row>  newRDD = documents.map(x -> x._2).map((Function<BSONObject, Row>) record -> {
            final String attributes = (String) record.get("identificationNumber");
            final String steps = (String) record.get("beat");
            final Date date = dateParser.parse((String)record.get("date"));
            return RowFactory.create(attributes, steps, new java.sql.Timestamp(date.getTime()));
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(newRDD, schema);
        peopleDataFrame.show();

        peopleDataFrame.createOrReplaceTempView("hearbeats");

        Dataset<Row> results = spark.sql("SELECT identifier, Avg(steps) as average, date  FROM hearbeats GROUP BY date, identifier order by identifier");
        results.show();

    }

    private static StructField returnStructField(String fieldName) {
        return Match(fieldName).of(
                 Case(is(DATE_TYPE), DataTypes.createStructField(fieldName, DataTypes.TimestampType, true)),
                 Case($(), DataTypes.createStructField(fieldName, DataTypes.StringType, true))
        );
    }

    private static JavaPairRDD<Object, BSONObject> getDocumentsFromDB() {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark shell");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Configuration mongodbConfig = new Configuration();
        mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
        mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/CPP.Hearbeats");
        return sc.newAPIHadoopRDD(
                mongodbConfig,
                MongoInputFormat.class,
                Object.class,
                BSONObject.class
        );
    }

    private static StructType getSchema() {
        String schemaString = "identifier;steps;date";
        List<StructField> fields = Arrays.stream(schemaString.split(SEPARATE_CHAR))
                .map(HearBeatAnalythics::returnStructField)
                .collect(Collectors.toList());
        return DataTypes.createStructType(fields);
    }


}