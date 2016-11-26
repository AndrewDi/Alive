package com.andrew;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Andrew on 26/11/2016.
 */
@RestController
@EnableAutoConfiguration
@RequestMapping("/rest")
public class RestControllerApi {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    AliveSchedule aliveSchedule;
    public RestControllerApi(){
        this.aliveSchedule=AliveMain.aliveSchedule;
    }

    @RequestMapping("/getalllist")
    public ConcurrentHashMap<String,DB2InfoModel> getAllList(){
        return this.aliveSchedule.getDb2List();
    }

    @RequestMapping(value = "/search/{key}",method = RequestMethod.POST)
    public Object[] getListBySearch(@PathVariable("key") String key){
        return this.aliveSchedule.getDb2List().values().stream().filter(db2InfoModel -> db2InfoModel.getDBName().toUpperCase().contains(key.toUpperCase())||db2InfoModel.getIP().toUpperCase().contains(key.toUpperCase())).toArray();
    }

    @RequestMapping(value = "/reset/{jobkey}",method = RequestMethod.POST)
    public Boolean resetRetry(@PathVariable("jobkey") String jobkey){
        log.info("Reset Current Retry Times,JobKey:"+jobkey);
        return this.aliveSchedule.resetRetry(jobkey);
    }
}