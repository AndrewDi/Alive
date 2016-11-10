package com.andrew;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Andrew on 27/10/2016.
 */
public class DB2Info {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public ConcurrentHashMap<String,DB2InfoModel> getDB2InfoList(){
        Connection connection= null;
        ConcurrentHashMap<String,DB2InfoModel> db2List = new ConcurrentHashMap<>();
        String passCol ="DBPASS";
        if(!AppConf.getConf().getEncryptPass().trim().isEmpty()){
            passCol="DECRYPT_CHAR(CAST(DBPASS as VARCHAR(64) FOR BIT DATA),?) AS DBPASS";
        }
        String sql = String.format("SELECT LDBID,VIP,PORT,DBNAME,VALID,DECODE(dbalias,null,DBNAME,dbalias) as DBALIAS,DBUSER,%s,UIDAPP FROM CMDB.LDBINFO WHERE UIDFLAG='%s' AND VALID in ('Y','M') AND VIP IS NOT NULL AND PORT IS NOT NULL AND DBNAME IS NOT NULL AND DBUSER IS NOT NULL AND ((UIDAPP='%s' OR UIDAPP IS NULL) OR (UIDAPP!='%s' AND (UIDLASTUPDATETIME IS NULL OR UIDLASTUPDATETIME < current timestamp - %d MINUTES)))",passCol,AppConf.getConf().getUid_flag(),AppConf.getConf().getAppFlag(),AppConf.getConf().getAppFlag(),AppConf.getConf().getMax_allow_ha_interval());
        try {
            log.debug(sql);
            connection = ConnectionUtils.getConnection(AppConf.getConf().getDbmdb_ip(),AppConf.getConf().getDbmdb_port(),AppConf.getConf().getDbmdb_dbname(),AppConf.getConf().getDbmdb_username(),AppConf.getConf().getDbmdb_passwd());
            PreparedStatement ps = connection.prepareStatement(sql);
            if(!AppConf.getConf().getEncryptPass().trim().isEmpty()){
                ps.setString(1,AppConf.getConf().getEncryptPass());
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next())
            {
                int id=0;
                for(String vip:rs.getString("VIP").split(",")) {
                    DB2InfoModel db2InfoModel = new DB2InfoModel(id,rs.getString("LDBID"),vip,rs.getInt("PORT"),rs.getString("DBNAME"),rs.getString("DBALIAS"),rs.getString("DBUSER"),rs.getString("DBPASS"));
                    db2InfoModel.setUIDApp(rs.getString("UIDAPP"));
                    db2List.put(db2InfoModel.toString(),db2InfoModel);
                    id++;
                }
            }
            rs.close();
            ps.close();
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
