package com.andrew;

import java.util.Properties;

/**
 * Created by Andrew on 27/10/2016.
 */
public class AppConf {

    private Properties properties;
    private static AppConf appConf;

    public static AppConf getConf(){
        if(null==appConf)
        {
            appConf=new AppConf();
            appConf.Init();
        }
        return appConf;
    }

    public void Init(){
        properties = ConfigurationUtils.getConf();
        this.appFlag = properties.get("appflag").toString();
        this.dbmdb_ip = properties.get("dbmdb.ip").toString();
        this.dbmdb_port = Integer.valueOf(properties.get("dbmdb.port").toString());
        this.dbmdb_dbname = properties.get("dbmdb.dbname").toString();
        this.dbmdb_username = properties.get("dbmdb.username").toString();
        this.dbmdb_passwd = properties.get("dbmdb.passwd").toString();
        this.encryptPass = properties.get("encryptPass").toString();
        this.isuseshortconnection = Integer.valueOf(properties.get("isuseshortconnection").toString());
        this.threadpool = Integer.valueOf(properties.get("threadpool").toString());
        this.interruptmillisecond = Long.valueOf(properties.get("interruptmillisecond").toString());
        this.intervalInSeconds = Integer.valueOf(properties.get("intervalInSeconds").toString());
        this.maxRetries = Integer.valueOf(properties.get("maxRetries").toString());
        this.ip_exception_list=properties.get("ip_exception_list").toString().split(",");
        this.ip_allow_list=properties.get("ip_allow_list").toString().split(",");
        this.refresh_dbmdb_interval=Integer.valueOf(properties.get("refresh_dbmdb_interval").toString());
        this.max_allow_ha_interval=Integer.valueOf(properties.get("max_allow_ha_interval").toString());
        this.uid_flag=properties.get("uidflag").toString();
    }

    private String appFlag;
    private String dbmdb_ip;
    private int dbmdb_port;
    private String dbmdb_dbname;
    private String dbmdb_username;
    private String dbmdb_passwd;
    private String encryptPass;
    private int isuseshortconnection;
    private int threadpool;
    private long interruptmillisecond;
    private int intervalInSeconds;
    private int maxRetries;
    private String[] ip_exception_list;
    private String[] ip_allow_list;
    private int refresh_dbmdb_interval;
    private int max_allow_ha_interval;
    private String uid_flag;

    public String getAppFlag() {
        if(appFlag==null||appFlag.isEmpty()) {
            if (System.getenv("HOSTNAME") != null)
                this.appFlag = System.getenv("HOSTNAME");
            else
                this.appFlag = "UIDSnap";
        }
        return this.appFlag;
    }

    public String getDbmdb_ip() {
        return dbmdb_ip;
    }

    public int getDbmdb_port() {
        return dbmdb_port;
    }

    public String getDbmdb_dbname() {
        return dbmdb_dbname;
    }

    public String getDbmdb_username() {
        return dbmdb_username;
    }

    public String getDbmdb_passwd() {
        return dbmdb_passwd;
    }

    public String getEncryptPass() {
        return encryptPass;
    }

    public int getIsuseshortconnection() {
        return isuseshortconnection;
    }

    public int getThreadpool() {
        return threadpool;
    }

    public long getInterruptmillisecond() {
        return interruptmillisecond;
    }

    public int getIntervalInSeconds() {
        return intervalInSeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String[] getIp_exception_list() {
        return ip_exception_list;
    }

    public String[] getIp_allow_list() {
        return ip_allow_list;
    }

    public int getRefresh_dbmdb_interval() {
        return refresh_dbmdb_interval;
    }

    public int getMax_allow_ha_interval() {
        return max_allow_ha_interval;
    }

    public String getUid_flag() {
        return uid_flag;
    }
}
