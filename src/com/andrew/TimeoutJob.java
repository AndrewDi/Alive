package com.andrew;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Created by Andrew on 30/10/2016.
 */
public class TimeoutJob implements Job {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        Scheduler scheduler = jobExecutionContext.getScheduler();
        try {
            if(!scheduler.isStarted())
                return;
            List<JobExecutionContext> jobExecutionContextList = scheduler.getCurrentlyExecutingJobs();
            for(JobExecutionContext jec:jobExecutionContextList){
                if(jec.getClass()==this.getClass()||jec.getClass().toString()==RefreshDB2InfoList.class.toString()||jec.getClass().toString()==UpdateUIDStatusJob.class.toString()) {
                    continue;
                }
                if(!jec.getJobDetail().getJobDataMap().containsKey("startTime")){
                    continue;
                }
                long startTime = jec.getJobDetail().getJobDataMap().getLong("startTime");
                long currentTime = System.currentTimeMillis();
                if((currentTime-startTime) > AppConf.getConf().getInterruptmillisecond()){
                    log.error(String.format("Job %s Has Been running %d,will be Interrupt",jec.getJobDetail().getKey(),(currentTime-startTime)));
                    jec.getJobDetail().getJobDataMap().put("Interrupt",true);
                    scheduler.interrupt(jec.getJobDetail().getKey());
                }
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
    }
}
