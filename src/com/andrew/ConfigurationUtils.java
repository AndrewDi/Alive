package com.andrew;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Created by Andrew on 27/10/2016.
 */
public class ConfigurationUtils {
    public static Properties getConf(){
        Properties props = new Properties();
        HashMap<String,Object> confs=new HashMap<String, Object>();
        File file=new File("conf/appconf.properties");
        String path = null;
        InputStream in = null;
        try {
            if(file.exists()){
                path = file.getAbsolutePath();
                in = new FileInputStream(path);
            }
            else {
                in = ClassLoader.getSystemResourceAsStream("appconf.properties");
            }
            props.load(in);
            return props;
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return null;
    }

    public static String getQuartzConf(){
        File file=new File("conf/quartz.properties");
        if(file.exists())
        {
            return file.getAbsolutePath();
        }
        return null;
    }

    public static String getLoggerConf(){
        File file=new File("conf/logback.xml");
        if(file.exists())
        {
            return file.getAbsolutePath();
        }
        return null;
    }
}
