package com.andrew;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Andrew on 27/10/2016.
 */
public class DB2Info {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public ConcurrentHashMap<String,DB2InfoModel> getDB2InfoList(){
        Connection connection= null;
        ConcurrentHashMap<String,DB2InfoModel> db2List = new ConcurrentHashMap<>();
        String sql="SELECT LDB.LDBID,LDB.DBNAME,LDB.VIP AS VIPLIST,LDB.PORT,LDB.DBUSER,DECODE(?,' ',LDB.DBPASS,DECRYPT_CHAR(CAST(LDB.DBPASS AS VARCHAR(64) FOR BIT DATA),?)) AS DBPASS,DECODE(LDB.DBALIAS,NULL,LDB.DBNAME,LDB.DBALIAS) AS DBALIAS,LDB.VALID,UID.VIP,UID.STATUS,UID.LASTUPDATETIME,UID.UIDAPP " +
                "FROM CMDB.LDBINFO AS LDB FULL JOIN DBI.DBUID AS UID ON (LDB.LDBID,LDB.DBNAME)=(UID.LDBID,UID.DBNAME)" +
                "WHERE LDB.VALID IN ('Y','M') AND LDB.VIP IS NOT NULL AND LDB.PORT IS NOT NULL AND LDB.DBNAME IS NOT NULL AND LDB.DBUSER IS NOT NULL " +
                "AND (UID.UIDAPP=? OR UID.UIDAPP IS NULL OR (UID.UIDAPP!=? AND (UID.LASTUPDATETIME IS NULL OR UID.LASTUPDATETIME < CURRENT TIMESTAMP - ? MINUTES)))";
        String SQL_CMDB_LDBINFO="SELECT LDBID,DBNAME,VIP AS VIPLIST,PORT,DBUSER,DECODE(?,' ',DBPASS,DECRYPT_CHAR(CAST(DBPASS AS VARCHAR(64) FOR BIT DATA),?)) AS DBPASS,DECODE(DBALIAS,NULL,DBNAME,DBALIAS) AS DBALIAS,VALID " +
                "FROM CMDB.LDBINFO WHERE UIDFLAG=? AND VALID IN ('Y','M') AND VIP IS NOT NULL AND PORT IS NOT NULL AND DBNAME IS NOT NULL AND DBUSER IS NOT NULL";
        String SQL_UID_DBUID="SELECT LDBID,DBNAME,PORT,VIP,STATUS,TIMESTAMPDIFF(2,CHAR(CURRENT TIMESTAMP - NVL(LASTUPDATETIME,CURRENT TIMESTAMP - 30 MINUTES))) as TONOW,UIDAPP FROM DBI.DBUID ";
        String SQL_DELETE_DBUID="DELETE FROM DBI.DBUID WHERE LDBID=? AND DBNAME=? AND VIP=?";
        try {
            log.debug(SQL_CMDB_LDBINFO);
            log.debug(SQL_UID_DBUID);
            connection = ConnectionUtils.getConnection(AppConf.getConf().getDbmdb_ip(),AppConf.getConf().getDbmdb_port(),AppConf.getConf().getDbmdb_dbname(),AppConf.getConf().getDbmdb_username(),AppConf.getConf().getDbmdb_passwd());
            PreparedStatement ps = connection.prepareStatement(SQL_CMDB_LDBINFO);
            if(!AppConf.getConf().getEncryptPass().trim().isEmpty()){
                ps.setString(1,AppConf.getConf().getEncryptPass());
                ps.setString(2,AppConf.getConf().getEncryptPass());
            }
            else {
                ps.setString(1," ");
                ps.setString(2," ");
            }
            ps.setString(3,AppConf.getConf().getUid_flag());
            ResultSet rs = ps.executeQuery();
            while (rs.next())
            {
                int id=0;
                for(String vip:rs.getString("VIPLIST").split(",")) {
                    DB2InfoModel db2InfoModel = new DB2InfoModel(id,rs.getString("LDBID"),vip,rs.getInt("PORT"),rs.getString("DBNAME"),rs.getString("DBALIAS"),rs.getString("DBUSER"),rs.getString("DBPASS"));
                    db2InfoModel.setVIPList(rs.getString("VIPLIST"));
                    db2List.put(db2InfoModel.toString(),db2InfoModel);
                    id++;
                }
            }
            rs.close();
            ps.close();
            rs=connection.createStatement().executeQuery(SQL_UID_DBUID);
            while (rs.next()){
                String LDBID=rs.getString("LDBID");
                String DBNAME=rs.getString("DBNAME");
                int PORT=rs.getInt("PORT");
                String VIP=rs.getString("VIP");
                int STATUS=rs.getInt("STATUS");
                int TONOW=rs.getInt("TONOW");
                String UIDAPP=rs.getString("UIDAPP");
                String uidString = DB2InfoModel.makeString(DBNAME,LDBID,VIP,PORT);
                String myAppFlag = AppConf.getConf().getAppFlag();
                if(UIDAPP.equals(myAppFlag)&&db2List.containsKey(uidString)){
                    db2List.get(uidString).setUIDApp(UIDAPP);
                    db2List.get(uidString).setSQLCode(STATUS);
                }
                else if(!UIDAPP.equals(myAppFlag)&&TONOW>AppConf.getConf().getMax_allow_ha_interval()*60&&db2List.containsKey(uidString)){
                    db2List.get(uidString).setUIDApp(UIDAPP);
                }
                else if(!UIDAPP.equals(myAppFlag)&&TONOW<=AppConf.getConf().getMax_allow_ha_interval()*60&&db2List.containsKey(uidString)){
                    db2List.remove(uidString);
                }
                else if(!db2List.containsKey(uidString)){
                    PreparedStatement psDelete = connection.prepareStatement(SQL_DELETE_DBUID);
                    psDelete.setString(1,LDBID);
                    psDelete.setString(2,DBNAME);
                    psDelete.setString(3,VIP);
                    psDelete.execute();
                    psDelete.close();
                }
            }
            rs.close();
            connection.close();
        } catch (SQLException e) {
            log.error(e.getMessage().toString());
        }
        finally {
            try {
                if(null!=connection) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage().toString());
            }
        }
        return db2List;
    }
}
