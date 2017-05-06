package com.architecture.spark.job;

import com.architecture.spark.model.HearBeat;
import com.architecture.spark.services.Mongo;
import com.architecture.spark.services.impl.MongoImpl;
import com.architecture.spark.util.MongoFactory;
import com.mongodb.MongoClient;
import javaslang.control.Try;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class JobHearBeat implements org.quartz.Job {

    private static DateTime dateTime = new DateTime();
    private DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd:HH:mm:ss");
    private Mongo mongo = new MongoImpl();

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        Try<MongoClient> mongoClient = MongoFactory.CONNECTION.getClient();

        System.out.println("    Start --" + new Date());
        dateTime = dateTime.plusSeconds(3);

        HearBeat hearBeat = new HearBeat(String.valueOf(ThreadLocalRandom.current().nextInt(80, 120 + 1)),String.valueOf(1030583889),dateTime.toString(dtf));

        mongo.saveObject(hearBeat,"Hearbeats",mongoClient);

    }
}
