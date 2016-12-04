package com.andrew;

import com.ibm.db2.jcc.DB2SimpleDataSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Andrew on 27/10/2016.
 */
public class ConnectionUtils {

    public static Connection getConnection(String ip, int port, String dbname, String user, String passwd) throws SQLException {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        }
        catch (ClassNotFoundException e){
            return null;
        }
        DB2SimpleDataSource db2DataSource = new DB2SimpleDataSource();
        db2DataSource.setDriverType(4);
        db2DataSource.setServerName(ip);
        db2DataSource.setPortNumber(port);
        db2DataSource.setUser(user);
        db2DataSource.setDatabaseName(dbname);
        db2DataSource.setRetrieveMessagesFromServerOnGetMessage(true);
        db2DataSource.setPassword(passwd);
        db2DataSource.setClientProgramName("UID_Alive");
        db2DataSource.setConnectionTimeout(10);
        db2DataSource.setClientWorkstation(AppConf.getConf().getAppFlag());
        return db2DataSource.getConnection();
    }

    public static Boolean IsReachable(String ip,int port){
        Boolean isReachable = false;
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(ip,port),5000);
            isReachable = socket.isConnected();
        } catch (IOException e) {
            return isReachable;
        }
        finally {
            try {
                if(!socket.isClosed())
                    socket.close();
            } catch (IOException e) {
                socket=null;
                e.printStackTrace();
            }
        }
        return isReachable;
    }

    public static String FindFirstUsableIp(String iplist,int port){
        for(String ip:iplist.split(",")){
            if(IsReachable(ip,port)){
                return ip;
            }
        }
        return null;
    }
}