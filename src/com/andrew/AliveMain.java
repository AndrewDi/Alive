package com.andrew;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@EnableAutoConfiguration
public class AliveMain {

    private static final Logger log = LoggerFactory.getLogger(AliveMain.class);
    public static AliveSchedule aliveSchedule = null;

    public AliveMain(){
        aliveSchedule = new AliveSchedule();
    }

    @Override
    protected void finalize() throws Throwable {
        aliveSchedule.stop();
        super.finalize();
    }

    public static void main(String[] args) {
        SpringApplication.run(AliveMain.class, args);
        aliveSchedule.start();
    }
}