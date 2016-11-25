package com.andrew;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Andrew on 27/10/2016.
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class RefreshDB2InfoList implements Job {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private Connection connection=null;
    private JobDataMap jobDataMap = null;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("Start to refresh db2 job list");
        this.jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        AliveSchedule aliveSchedule = (AliveSchedule)this.jobDataMap.get(AliveSchedule.class.toString());
        aliveSchedule.refreshDB2InfoList();
        /**
        ConcurrentLinkedQueue<DB2InfoModel> updateList = aliveSchedule.updateLastTimestampQueue;
        if(this.jobDataMap.containsKey("DBConn")){this.connection=(Connection) this.jobDataMap.get("DBConn");}
        for(DB2InfoModel db2InfoModel:updateList){
            if(this.update(db2InfoModel)){
                updateList.remove(db2InfoModel);
            }
        }
         **/
        log.info("End refresh db2 job list");
    }

    private Boolean update(DB2InfoModel db2InfoModel){
        PreparedStatement ps = null;
        try {
            if(this.connection==null||this.connection.isClosed())
            {
                this.connection = ConnectionUtils.getConnection(AppConf.getConf().getDbmdb_ip(),AppConf.getConf().getDbmdb_port(),AppConf.getConf().getDbmdb_dbname(),AppConf.getConf().getDbmdb_username(),AppConf.getConf().getDbmdb_passwd());
                if(this.jobDataMap.containsKey("DBConn")){
                    this.jobDataMap.replace("DBConn",this.connection);
                }
                else {
                    this.jobDataMap.put("DBConn",this.connection);
                }
            }
            String sqlString="UPDATE CMDB.LDBINFO SET UIDLASTUPDATETIME=CURRENT TIMESTAMP,UIDAPP=? WHERE LDBID=? AND DBNAME=?";
            ps = connection.prepareStatement(sqlString);
            ps.setString(1,AppConf.getConf().getAppFlag());
            ps.setString(2,db2InfoModel.getLDBID());
            ps.setString(3,db2InfoModel.getDBName());
            ps.execute();
            ps.close();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
