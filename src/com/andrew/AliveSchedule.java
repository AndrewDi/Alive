package com.andrew;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;


/**
 * Created by Andrew on 27/10/2016.
 */
public class AliveSchedule {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private DB2InfoList db2InfoList = null;
    public ConcurrentLinkedQueue<DB2InfoModel> updateLastTimestampQueue = null;
    private ConcurrentLinkedQueue<DB2InfoModel> updateUIDStatusQueue = null;
    private ConcurrentHashMap<String,Connection> uidJobConnections = null;

    private Scheduler scheduler = null;

    public AliveSchedule(){
        log.info("Start Init AliveSchedule");
        StdSchedulerFactory factory = new StdSchedulerFactory();
        try {
            if(ConfigurationUtils.getQuartzConf()!=null)
            {
                factory.initialize(ConfigurationUtils.getQuartzConf());

            }
            scheduler = factory.getScheduler();
            db2InfoList = new DB2InfoList();
            //updateLastTimestampQueue = new ConcurrentLinkedQueue<DB2InfoModel>();
            updateUIDStatusQueue = new ConcurrentLinkedQueue<DB2InfoModel>();
            uidJobConnections = new ConcurrentHashMap<>();
        } catch (SchedulerException e) {
            log.error(e.getMessage().toString());
        }
        try {
            if(ConfigurationUtils.getLoggerConf()!=null) {
                log.info(String.format("Using Log Conf %s instead",ConfigurationUtils.getLoggerConf()));
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                JoranConfigurator jc = new JoranConfigurator();
                jc.setContext(context);
                context.reset();
                jc.doConfigure(ConfigurationUtils.getLoggerConf());
            }
        } catch (JoranException e) {
            log.error(e.getMessage().toString());
        }
        log.info("End Init AliveSchedule");
    }

    public boolean start() {
        log.info(String.format("AliveSchedule is just about to Start, Appflag:%s",AppConf.getConf().getAppFlag()));
        try {
            if (null != this.scheduler) {
                this.scheduler.start();
            }
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
            return false;
        }
        this.startRefreshJob();
        this.startUpdateUIDStatusJob();
        this.startTimeoutJob();
        log.info("AliveSchedule Start Successful");
        return true;
    }

    private void startTimeoutJob(){
        JobDetail jobDetail = newJob(TimeoutJob.class)
                .withIdentity(TimeoutJob.class.toString())
                .build();
        log.info("Build Job "+ jobDetail.getKey().getName());

        Trigger trigger = newTrigger()
                .withIdentity(TimeoutJob.class.toString())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(1)
                        .repeatForever())
                .build();
        log.info("Build Trigger " + trigger.getKey().getName());
        try {
            this.scheduler.scheduleJob(jobDetail,trigger);
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
        }
    }

    private void startUpdateUIDStatusJob(){
        JobDetail jobDetail = newJob(UpdateUIDStatusJob.class)
                .withIdentity(UpdateUIDStatusJob.class.toString())
                .build();
        jobDetail.getJobDataMap().put(UpdateUIDStatusJob.class.toString(),this.updateUIDStatusQueue);
        log.info("Build Job "+ jobDetail.getKey().getName());

        Trigger trigger = newTrigger()
                .withIdentity(UpdateUIDStatusJob.class.toString())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(1)
                        .repeatForever())
                .build();
        log.info("Build Trigger " + trigger.getKey().getName());
        try {
            this.scheduler.scheduleJob(jobDetail,trigger);
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
        }
    }

    private void startRefreshJob(){
        JobDetail jobDetail = newJob(RefreshDB2InfoList.class)
                .withIdentity(this.getClass().toString())
                .build();
        jobDetail.getJobDataMap().put(this.getClass().toString(),this);
        log.info("Build Job "+ jobDetail.getKey().getName());

        Trigger trigger = newTrigger()
                .withIdentity(this.getClass().toString())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(AppConf.getConf().getRefresh_dbmdb_interval())
                        .repeatForever())
                .build();
        log.info("Build Trigger " + trigger.getKey().getName());
        try {
            this.scheduler.scheduleJob(jobDetail,trigger);
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
        }
    }

    public boolean stop(){
        log.info("AliveSchedule is just about to Stop");
        try {
            if (null != this.scheduler) {
                this.scheduler.clear();
                this.scheduler.shutdown();
            }
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
            return false;
        }
        log.info("AliveSchedule Stop Successful");
        return true;
    }

    public boolean deleteJob(String jobKey){
        try {
            this.scheduler.getJobDetail(new JobKey(jobKey)).getJobDataMap().put("Delete_FLAG",true);
            this.scheduler.interrupt(new JobKey(jobKey));
            this.scheduler.pauseTrigger(new TriggerKey(jobKey));
            this.scheduler.unscheduleJob(new TriggerKey(jobKey));
            this.removeConnection(jobKey);
            this.db2InfoList.RemoveDB2Info(jobKey);
        } catch (SchedulerException e) {
            log.error(e.getMessage().toString());
            return false;
        }
        return true;
    }

    public void addConnection(String jobkey,Connection connection){
        this.removeConnection(jobkey);
        this.uidJobConnections.put(jobkey, connection);
    }

    public void removeConnection(String jobkey){
        if(this.uidJobConnections.containsKey(jobkey)){
            Connection conn = this.uidJobConnections.get(jobkey);
            try {
                if(null!=conn&&!conn.isClosed()){
                    conn.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage().toString());
            }
            this.uidJobConnections.remove(jobkey);
        }
    }

    public boolean AddJob(DB2InfoModel db2InfoModel){
        this.db2InfoList.AddDB2Info(db2InfoModel);
        JobDetail jobDetail = newJob(UIDJob.class)
                .withIdentity(db2InfoModel.toString())
                .build();
        jobDetail.getJobDataMap().put(this.getClass().toString(),this);
        jobDetail.getJobDataMap().put(DB2InfoModel.class.toString(),db2InfoModel);
        log.info("Build Job "+ jobDetail.getKey().getName());

        Trigger trigger = newTrigger()
                .withIdentity(db2InfoModel.toString())
                .startNow()
                .withSchedule(simpleSchedule()
                        .withIntervalInSeconds(AppConf.getConf().getIntervalInSeconds())
                        .repeatForever())
                .build();
        log.info("Build Trigger " + trigger.getKey().getName());
        //this.updateLastTimestampQueue.add(db2InfoModel);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            log.error(e.getStackTrace().toString());
            return false;
        }
        return true;
    }

    public void AddUpdateUIDStatusJob(DB2InfoModel db2InfoModel){
        this.updateUIDStatusQueue.add(db2InfoModel);
    }

    public void refreshDB2InfoList(){
        DB2Info db2Info = new DB2Info();
        ConcurrentHashMap<String,DB2InfoModel> db2InfoArrayList= db2Info.getDB2InfoList();
        /**
        db2InfoArrayList.entrySet().stream().parallel().forEach(entry->{
            if(this.db2InfoList.getDB2Info(entry.getValue().toString())==null&&entry.getValue().getUIDApp().equals(AppConf.getConf().getAppFlag())&&ConnectionUtils.IsReachable(entry.getValue().getIP(),entry.getValue().getPort())){
                this.db2InfoList.AddDB2Info(entry.getValue());
                this.AddJob(entry.getValue());
                log.info("Add new Job to List:"+entry.getValue().toString());
            }
            else if (this.db2InfoList.getDB2Info(entry.getValue().toString())!=null&&entry.getValue().getUIDApp().equals(AppConf.getConf().getAppFlag())&&!this.db2InfoList.getDB2Info(entry.getValue().toString()).equals(entry.getValue())){
                this.deleteJob(entry.getValue().toString());
                this.db2InfoList.ReplaceDB2Info(entry.getValue());
                this.AddJob(entry.getValue());
                log.info("Update Job in List:"+entry.getValue().toString());
            }
        });
         **/
        for(DB2InfoModel db2InfoModel:db2InfoArrayList.values()){
            if(this.db2InfoList.getDB2Info(db2InfoModel.toString())==null&&db2InfoModel.getUIDApp().equals(AppConf.getConf().getAppFlag())&&ConnectionUtils.IsReachable(db2InfoModel.getIP(),db2InfoModel.getPort())){
                this.db2InfoList.AddDB2Info(db2InfoModel);
                this.AddJob(db2InfoModel);
                log.info("Add new Job to List:"+db2InfoModel.toString());
            }
            else if (this.db2InfoList.getDB2Info(db2InfoModel.toString())!=null&&db2InfoModel.getUIDApp().equals(AppConf.getConf().getAppFlag())&&!this.db2InfoList.getDB2Info(db2InfoModel.toString()).equals(db2InfoModel)){
                this.deleteJob(db2InfoModel.toString());
                this.db2InfoList.ReplaceDB2Info(db2InfoModel);
                this.AddJob(db2InfoModel);
                log.info("Update Job in List:"+db2InfoModel.toString());
            }
        }
        this.db2InfoList.getDb2List().values().stream().filter(db2InfoModel -> !db2InfoArrayList.containsKey(db2InfoModel.toString())||(db2InfoArrayList.containsKey(db2InfoModel.toString())&&!db2InfoModel.getUIDApp().equals(AppConf.getConf().getAppFlag()))).forEach(db2InfoModel -> {
            this.deleteJob(db2InfoModel.toString());
            this.db2InfoList.RemoveDB2Info(db2InfoModel);
            log.info("Delete job from List:" + db2InfoModel.toString() + " Job has Been running on " + db2InfoModel.getUIDApp());
        });
    }

    public ConcurrentHashMap<String,DB2InfoModel> getDb2List(){
        return this.db2InfoList.getDb2List();
    }

    public boolean resetRetry(String jobkey){
        if(this.db2InfoList.getDB2Info(jobkey)==null)
            return false;
        this.db2InfoList.getDB2Info(jobkey).setMaxRetry(0);
        return true;
    }

    public DB2InfoModel getDB2InfoModelByJobkey(String jobkey){
        return this.db2InfoList.getDB2Info(jobkey);
    }
}