package com.architecture.spark.job;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.net.UnknownHostException;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class ConfigureJobHearBeat {

    public static void main(String[] args) throws InterruptedException, UnknownHostException, SchedulerException {
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        scheduler.start();

        JobDetail job = newJob(JobHearBeat.class)
                .withIdentity("job", "group")
                .build();

        Trigger trigger = newTrigger()
                .withIdentity("trigger", "group")
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInMilliseconds(250)
                        .repeatForever())
                .build();

        scheduler.scheduleJob(job, trigger);
    }
}
