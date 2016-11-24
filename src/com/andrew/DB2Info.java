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
        String sql="select ldb.ldbid,ldb.dbname,ldb.vip as viplist,ldb.port,ldb.dbuser,DECODE(?,' ',ldb.DBPASS,DECRYPT_CHAR(CAST(ldb.DBPASS as VARCHAR(64) FOR BIT DATA),?)) as DBPASS,DECODE(ldb.dbalias,null,ldb.DBNAME,ldb.dbalias) as DBALIAS,ldb.valid,uid.vip,uid.status,uid.lastupdatetime,uid.uidapp " +
                "from cmdb.ldbinfo as ldb full join dbi.dbuid as uid on (ldb.ldbid,ldb.dbname)=(uid.ldbid,uid.dbname)" +
                "WHERE ldb.valid in ('Y','M') AND ldb.vip IS NOT NULL AND ldb.port IS NOT NULL AND ldb.dbname IS NOT NULL AND ldb.dbuser IS NOT NULL " +
                "AND (uid.uidapp=? OR uid.uidapp IS NULL OR (uid.uidapp!=? AND (uid.lastupdatetime IS NULL OR uid.lastupdatetime < current timestamp - ? MINUTES)))";
        try {
            log.debug(String.format("select ldb.ldbid,ldb.dbname,ldb.vip as viplist,ldb.port,ldb.dbuser,DBPASS,DECODE(ldb.dbalias,null,ldb.DBNAME,ldb.dbalias) as DBALIAS,ldb.valid,uid.vip,uid.status,uid.lastupdatetime,uid.uidapp " +
                    "                from cmdb.ldbinfo as ldb full join dbi.dbuid as uid on (ldb.ldbid,ldb.dbname)=(uid.ldbid,uid.dbname) " +
                    "                WHERE ldb.valid in ('Y','M') AND ldb.vip IS NOT NULL AND ldb.port IS NOT NULL AND ldb.dbname IS NOT NULL AND ldb.dbuser IS NOT NULL" +
                    "                AND (uid.uidapp='%s' OR uid.uidapp IS NULL OR (uid.uidapp!='%s' AND (uid.lastupdatetime IS NULL OR uid.lastupdatetime < current timestamp - %d MINUTES)))",AppConf.getConf().getAppFlag(),AppConf.getConf().getAppFlag(),AppConf.getConf().getMax_allow_ha_interval()));
            connection = ConnectionUtils.getConnection(AppConf.getConf().getDbmdb_ip(),AppConf.getConf().getDbmdb_port(),AppConf.getConf().getDbmdb_dbname(),AppConf.getConf().getDbmdb_username(),AppConf.getConf().getDbmdb_passwd());
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!AppConf.getConf().getEncryptPass().trim().isEmpty()){
                ps.setString(1,AppConf.getConf().getEncryptPass());
                ps.setString(2,AppConf.getConf().getEncryptPass());
            }
            else {
                ps.setString(1," ");
                ps.setString(2," ");
            }
            ps.setString(3,AppConf.getConf().getAppFlag());
            ps.setString(4,AppConf.getConf().getAppFlag());
            ps.setInt(5,AppConf.getConf().getMax_allow_ha_interval());
            ResultSet rs = ps.executeQuery();
            while (rs.next())
            {
                int id=0;
                if(rs.getString("VIP")==null||rs.getString("VIP").isEmpty()){
                    for(String vip:rs.getString("VIPLIST").split(",")) {
                        DB2InfoModel db2InfoModel = new DB2InfoModel(id,rs.getString("LDBID"),vip,rs.getInt("PORT"),rs.getString("DBNAME"),rs.getString("DBALIAS"),rs.getString("DBUSER"),rs.getString("DBPASS"));
                        db2InfoModel.setUIDApp(rs.getString("UIDAPP"));
                        db2InfoModel.setVIPList(rs.getString("VIPLIST"));
                        db2List.put(db2InfoModel.toString(),db2InfoModel);
                        id++;
                    }
                }
                else if(rs.getString("VIPLIST").contains(rs.getString("VIP"))){
                    DB2InfoModel db2InfoModel = new DB2InfoModel(id,rs.getString("LDBID"),rs.getString("VIP"),rs.getInt("PORT"),rs.getString("DBNAME"),rs.getString("DBALIAS"),rs.getString("DBUSER"),rs.getString("DBPASS"));
                    db2InfoModel.setUIDApp(rs.getString("UIDAPP"));
                    db2InfoModel.setVIPList(rs.getString("VIPLIST"));
                    db2List.put(db2InfoModel.toString(),db2InfoModel);
                }
            }
            rs.close();
            ps.close();
            connection.close();
            for(DB2InfoModel db2InfoModel:db2List.values()){
                for(String vip:db2InfoModel.getVIPList().split(",")){
                    if(!db2List.containsKey(db2InfoModel.makeString(vip))){
                        DB2InfoModel newinfoModel = new DB2InfoModel(db2InfoModel.getId()+1,db2InfoModel.getLDBID(),vip,db2InfoModel.getPort(),db2InfoModel.getDBName(),db2InfoModel.getDBAlias(),db2InfoModel.getUser(),db2InfoModel.getPasswd());
                        db2InfoModel.setUIDApp(db2InfoModel.getUIDApp());
                        db2InfoModel.setVIPList(db2InfoModel.getVIPList());
                        db2List.put(newinfoModel.toString(),newinfoModel);
                    }
                }
            }
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
