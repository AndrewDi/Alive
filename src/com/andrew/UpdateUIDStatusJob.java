package com.andrew;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Andrew on 30/10/2016.
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class UpdateUIDStatusJob implements Job {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private ConcurrentLinkedQueue<DB2InfoModel> updateUIDStatusQueue = null;
    private Connection connection = null;
    public JobDataMap jobDataMap = null;
    private PreparedStatement ps = null;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        this.jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        this.updateUIDStatusQueue = (ConcurrentLinkedQueue<DB2InfoModel>)this.jobDataMap.get(this.getClass().toString());
        if(this.jobDataMap.containsKey("DBConn")){this.connection=(Connection) this.jobDataMap.get("DBConn");}
        try {
            if(this.connection==null||this.connection.isClosed())
            {
                this.connection = ConnectionUtils.getConnection(AppConf.getConf().getDbmdb_ip(),AppConf.getConf().getDbmdb_port(),AppConf.getConf().getDbmdb_dbname(),AppConf.getConf().getDbmdb_username(),AppConf.getConf().getDbmdb_passwd());
                if(this.jobDataMap.containsKey("DBConn")){
                    this.jobDataMap.replace("DBConn",connection);
                }
                else {
                    this.jobDataMap.put("DBConn",connection);
                }
            }
            this.ps = this.connection.prepareStatement("MERGE INTO DBI.DBUID T " +
                    "USING table(values(?,?,?,?,?,?,?,?,?)) S (VIP,PORT,DBNAME,STATUS,PHASE,LDBID,UIDAPP,MESSAGE,LASTCONNECTTIME) " +
                    "ON (T.VIP,T.PORT,T.DBNAME,T.LDBID)=(S.VIP,S.PORT,S.DBNAME,S.LDBID) " +
                    "WHEN MATCHED AND S.STATUS=-6 THEN UPDATE SET STATUS=S.STATUS,LASTUPDATETIME=CURRENT TIMESTAMP,PHASE=S.PHASE,LASTCONNECTTIME=S.LASTCONNECTTIME " +
                    "WHEN MATCHED THEN UPDATE SET STATUS=S.STATUS,LASTUPDATETIME=CURRENT TIMESTAMP,PHASE=S.PHASE,MESSAGE=S.MESSAGE,LASTCONNECTTIME=S.LASTCONNECTTIME " +
                    "WHEN NOT MATCHED THEN INSERT (VIP,PORT,DBNAME,STATUS,LASTUPDATETIME,PHASE,LDBID,UIDAPP,MESSAGE,LASTCONNECTTIME)  VALUES (S.VIP,S.PORT,S.DBNAME,S.STATUS,CURRENT TIMESTAMP,S.PHASE,S.LDBID,S.UIDAPP,S.MESSAGE,S.LASTCONNECTTIME)");

            for(DB2InfoModel db2InfoModel:this.updateUIDStatusQueue) {
                ps.setString(1, db2InfoModel.getIP());
                ps.setInt(2, db2InfoModel.getPort());
                ps.setString(3, db2InfoModel.getDBName());
                ps.setInt(4, db2InfoModel.getSQLCode());
                ps.setString(5,db2InfoModel.getStatus());
                ps.setString(6,db2InfoModel.getLDBID());
                ps.setString(7,AppConf.getConf().getAppFlag());
                ps.setString(8,db2InfoModel.getMessage());
                ps.setString(9,db2InfoModel.getLastConnectTime());
                ps.addBatch();
                this.updateUIDStatusQueue.remove(db2InfoModel);
            }
            ps.executeBatch();
            ps.close();
            connection.commit();
        } catch (SQLException e) {
            while (e!=null){
                log.error(e.getMessage().toString());
                e=e.getNextException();
            }
            connection=null;
        }catch (Exception e){
            log.error(e.getMessage().toString());
            connection=null;
        }
    }
}
