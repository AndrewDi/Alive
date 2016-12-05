package com.andrew;

import com.ibm.db2.jcc.DB2Diagnosable;
import com.ibm.db2.jcc.DB2Sqlca;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Andrew on 27/10/2016.
 */
@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class UIDJob implements InterruptableJob {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private JobDataMap jobDataMap = null;
    private Connection connection = null ;
    private PreparedStatement ps = null;
    private ResultSet rs = null;
    private Statement sm =null;
    private int isuseshortconnection=1;
    private String appflag = "NoProduction";
    private DB2InfoModel db2InfoModel = null;
    private AliveSchedule aliveSchedule = null;
    private int SQLCode = 0 ;
    private String Status = "Init";
    private String Message = "";

    private final String STATUS_CONNECT = "Connect";
    private final String STATUS_CREATE_TABLE ="CreateTable";
    private final String STATUS_INSERT ="Insert";
    private final String STATUS_DELETE = "Delete";
    private final String STATUS_SUCCESS = "Success";
    private final String STATUS_REACH_MAXRETRY = "Reach_MAX_Retry_Limit";

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        if(!this.jobDataMap.containsKey("startTime")) {
            disconnect();
            return;
        }
        long startTime = this.jobDataMap.getLong("startTime");
        long endTime = System.currentTimeMillis();
        disconnect();
        this.db2InfoModel.setSQLCode(this.SQLCode);
        this.db2InfoModel.setStatus(this.Status);
        this.db2InfoModel.setMessage(String.format("Interrupt Job,Duration:%d",endTime-startTime));
        if(this.jobDataMap.containsKey("Delete_FLAG")&&this.jobDataMap.getBoolean("Delete_FLAG")){
            log.info("About to Delete job from List:" + db2InfoModel.toString());
            return;
        }
        if(aliveSchedule!=null) {
            aliveSchedule.AddUpdateUIDStatusJob(this.db2InfoModel);
        }
    }

    private Boolean createTab() throws SQLException {
        //Check If Test Table has been create
        sm = connection.createStatement();
        rs = sm.executeQuery("SELECT COUNT(*) as TABCOUNT FROM SYSCAT.TABLES WHERE TABNAME='UIDCHECK' AND TABSCHEMA='DBX' WITH CS");

        rs.next();
        int tabExists = rs.getInt("TABCOUNT");
        if(null!=rs){rs.close();}
        if(null!=sm){sm.close();}
        if (tabExists == 0)
        {
            log.info("CREATE TABLE DBA.TEST(ID VARCHAR(32),OPR CHAR(1),CHECKTIME TIMESTAMP)");
            return connection.createStatement().execute("CREATE TABLE DBX.UIDCHECK(ID VARCHAR(32),APPFLAG VARCHAR(32),CHECKTIME TIMESTAMP)");
        }
        return true;
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        this.db2InfoModel = (DB2InfoModel)jobDataMap.get(DB2InfoModel.class.toString());
        if(this.db2InfoModel.getValid().equals("N"))
            return;
        this.aliveSchedule = (AliveSchedule)jobDataMap.get(AliveSchedule.class.toString());
        if(!db2InfoModel.isPingable()) {
            log.debug("Checking connection for ip:"+db2InfoModel.getIP());
            db2InfoModel.setPingable(ConnectionUtils.IsReachable(db2InfoModel.getIP(), db2InfoModel.getPort()));
            if(!db2InfoModel.isPingable()) return;
        }
        if(!jobDataMap.containsKey("maxRetries")) jobDataMap.put("maxRetries",0);
        this.isuseshortconnection = AppConf.getConf().getIsuseshortconnection();
        this.appflag = AppConf.getConf().getAppFlag();
        long startTime = System.currentTimeMillis();
        if(jobDataMap.containsKey("startTime")) {
            jobDataMap.replace("startTime",startTime);
        }else {
            jobDataMap.put("startTime",startTime);
        }


        this.Status = STATUS_CONNECT;

        if(this.isuseshortconnection==0&&jobDataMap.containsKey("connection")){this.connection=(Connection) jobDataMap.get("connection");}
        try {
            if(AppConf.getConf().getMaxRetries()!=0&&this.db2InfoModel.getMaxRetry()>=AppConf.getConf().getMaxRetries()){
                throw new MAXRetryLimitException(this.db2InfoModel.getMaxRetry());
            }
            if (connection==null||connection.isClosed()){
                connection=ConnectionUtils.getConnection(db2InfoModel.getIP(),db2InfoModel.getPort(),db2InfoModel.getDBAlias(),db2InfoModel.getUser(),db2InfoModel.getPasswd());
                if(this.isuseshortconnection==0){jobDataMap.put("connection",connection);}
                if(null==this.connection){
                    throw new SQLException(String.format("Failed Init Connection For %s",db2InfoModel.toString()),"Connect-Failed",-2);
                }
                this.db2InfoModel.setLastConnectTime(LocalDateTime.now());
                this.aliveSchedule.addConnection(this.db2InfoModel.toString(),connection);
            }


            this.Status = STATUS_CREATE_TABLE;
            if(!createTab()) {
                throw new SQLException(String.format("Can not create table For %s",db2InfoModel.toString()),"Create-Failed",-3);
            }


            //Do Insert Test
            this.Status = STATUS_INSERT;
            ps = connection.prepareStatement("INSERT INTO DBX.UIDCHECK VALUES(?,?,CURRENT TIMESTAMP )");
            ps.setString(1,String.valueOf(db2InfoModel.getId()));
            ps.setString(2,this.appflag);
            ps.executeUpdate();
            ps.close();
            connection.commit();
            log.debug("INSERT INTO DBX.UIDCHECK VALUES("+db2InfoModel.getId()+",'I',CURRENT TIMESTAMP )");


            //Do Delete Test
            this.Status = STATUS_DELETE;
            ps = connection.prepareStatement("DELETE FROM DBX.UIDCHECK WHERE ID=? and APPFLAG=?");
            ps.setString(1,String.valueOf(db2InfoModel.getId()));
            ps.setString(2,appflag);
            ps.executeUpdate();
            ps.close();
            connection.commit();
            log.debug("DELETE FROM DBX.UIDCHECK WHERE ID="+db2InfoModel.getId());

            this.Status = STATUS_SUCCESS ;
        }catch (MAXRetryLimitException e){
            this.SQLCode=-6;
            this.Status = STATUS_REACH_MAXRETRY;
            this.disconnect();
        }
        catch (SQLException e) {
            //if(e.getSQLState()=="28000"&&db2InfoModel.getSQLCode()!=-6) {
            //    this.db2InfoModel.addRetry();
            //}
            this.SQLCode = e.getErrorCode();
            this.Message = e.getMessage().toString();
            if(e instanceof DB2Diagnosable)
            {
                DB2Diagnosable diagnosable = (DB2Diagnosable)e;
                DB2Sqlca sqlca = diagnosable.getSqlca();
                if(sqlca!=null){
                    this.SQLCode = sqlca.getSqlCode();
                    log.error(String.format("[%s] Catch SQLException SQLCode:%d SQLState:%s",db2InfoModel.toString(),this.SQLCode,this.Message));
                    //IF HADR Related Error,No more try
                    if(this.SQLCode==-1776||this.SQLCode==-1773){
                        log.error("[HADR] Found HADR IP:"+this.db2InfoModel.toFullString());
                        for(String vip:this.db2InfoModel.getVIPList().split(",")){
                            if(!vip.equals(db2InfoModel.getIP())){
                                try {
                                    connection=ConnectionUtils.getConnection(vip,this.db2InfoModel.getPort(),this.db2InfoModel.getDBAlias(),this.db2InfoModel.getUser(),this.db2InfoModel.getPasswd());
                                    if(connection!=null&&createTab()){
                                        connection.createStatement().execute("SELECT * FROM DBX.UIDCHECK FETCH FIRST 1 ROWS ONLY WITH CS");
                                        this.db2InfoModel.setIP(vip);
                                        log.info("[HADR] Found non-HADR ip:"+this.db2InfoModel.toFullString());
                                        break;
                                    }
                                }
                                catch (SQLException connectSQLException){
                                    this.disconnect();
                                }
                            }
                        }
                    }
                }
                else {
                    log.error(String.format("[%s] Catch SQLException ErrorCode:%d SQLState:%s",db2InfoModel.toString(),this.SQLCode,this.Message));
                }
            }else {
                if(e.getErrorCode()!=0) {
                    log.error(String.format("[%s] Catch SQLException ErrorCode:%d SQLState:%s", db2InfoModel.toString(),this.SQLCode,this.Message));
                }
            }
            this.disconnect();
            this.changeIP();
        }catch (Exception e){
            this.SQLCode=-4;
            if(!this.jobDataMap.containsKey("Interrupt")){
                this.Message=e.getMessage();
                log.error(String.format("[%s]%s", this.db2InfoModel.toString(), e.getMessage().toString()));
            }
            else {
                this.jobDataMap.remove("Interrupt");
            }
            this.disconnect();
            this.changeIP();
        }
        finally {
            this.db2InfoModel.setSQLCode(this.SQLCode);
            this.db2InfoModel.setStatus(this.Status);
            this.db2InfoModel.setMessage(this.Message);
            aliveSchedule.AddUpdateUIDStatusJob(this.db2InfoModel);
            if(this.isuseshortconnection==1){
                this.disconnect();
            }
        }
        long endTime = System.currentTimeMillis();
        log.debug(String.format("[%s] completed work at %d SQLCode:%d SQLState:%s",db2InfoModel.toString(),endTime-startTime,db2InfoModel.getSQLCode(),db2InfoModel.getStatus()));
    }

    private void disconnect()
    {
        try {
            if(null!=sm){sm.close();sm=null;}
            if(null!=ps){ps.close();ps=null;}
            if(null!=rs){rs.close();rs=null;}
            if(null!=connection&&!connection.isClosed()) {
                connection.close();
                connection=null;
            }
        } catch (SQLException e1) {
            log.error(String.format("%s:%s",db2InfoModel.toString(),e1.getMessage().toString()));
        }
    }

    private void changeIP(){
        if(ConnectionUtils.IsReachable(db2InfoModel.getIP(), db2InfoModel.getPort())){
            return;
        }
        else {
            this.db2InfoModel.setIP(ConnectionUtils.FindFirstUsableIp(this.db2InfoModel.getVIPList(),this.db2InfoModel.getPort()));
        }
    }
}
