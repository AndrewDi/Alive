package com.andrew;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.SortedMap;
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
        return this.aliveSchedule.getDb2List().entrySet().stream().filter(entry -> entry.getValue().toString().contains(key)).toArray();
    }

    @RequestMapping(value = "/reset/{jobkey}",method = RequestMethod.POST)
    public Boolean resetRetry(@PathVariable("jobkey") String jobkey){
        log.info("Reset Current Retry Times,JobKey:"+jobkey);
        return this.aliveSchedule.resetRetry(jobkey);
    }

    @RequestMapping(value = "/markinvalid/{jobkey}",method = RequestMethod.POST)
    public Boolean markInvalid(@PathVariable("jobkey") String jobkey){
        log.info("Mark Invalid,JobKey:"+jobkey);
        if(this.aliveSchedule.getDB2InfoModelByJobkey(jobkey)==null){
            return false;
        }
        this.aliveSchedule.getDB2InfoModelByJobkey(jobkey).setValid("N");
        return true;
    }

    @RequestMapping(value = "/markvalid/{jobkey}",method = RequestMethod.POST)
    public Boolean markValid(@PathVariable("jobkey") String jobkey){
        log.info("Mark valid,JobKey:"+jobkey);
        if(this.aliveSchedule.getDB2InfoModelByJobkey(jobkey)==null){
            return false;
        }
        this.aliveSchedule.getDB2InfoModelByJobkey(jobkey).setValid("Y");
        return true;
    }

    @RequestMapping("/status")
    public SortedMap<String,Object> getStatus(){
        SortedMap<String,Object> maps =  MemoryUsage.DisplayMemory();
        maps.put("CurrentExecuting",this.aliveSchedule.getDb2List().values().toArray().length);
        return maps;
    }
}