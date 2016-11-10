package com.andrew;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by Andrew on 27/10/2016.
 */
public class DB2InfoModel {
    private int id;
    private String LDBID;
    private String IP;
    private int Port;
    private String DBName;
    private String DBAlias;
    private String User;
    private String Passwd;
    private String valid;
    private int maxRetry=0;
    private boolean pingable = false;
    private int SQLCode = -1;
    private String Status;
    private String Message;
    private String UIDApp;
    private LocalDateTime LastConnectTime;

    public DB2InfoModel(int id, String LDBID, String IP, int port, String DBName, String DBAlias, String user, String passwd) {
        this.id = id;
        this.LDBID = LDBID;
        this.IP = IP;
        Port = port;
        this.DBName = DBName;
        this.DBAlias = DBAlias;
        User = user;
        Passwd = passwd;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLDBID() {
        return LDBID;
    }

    public void setLDBID(String LDBID) {
        this.LDBID = LDBID;
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public int getPort() {
        return Port;
    }

    public void setPort(int port) {
        Port = port;
    }

    public String getDBName() {
        return DBName;
    }

    public void setDBName(String DBName) {
        this.DBName = DBName;
    }

    public String getDBAlias() {
        return DBAlias;
    }

    public void setDBAlias(String DBAlias) {
        this.DBAlias = DBAlias;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        User = user;
    }

    public String getPasswd() {
        return Passwd;
    }

    public void setPasswd(String passwd) {
        Passwd = passwd;
    }


    public String getValid() {
        return valid;
    }

    public void setValid(String valid) {
        this.valid = valid;
    }

    public boolean isPingable() {
        return pingable;
    }

    public void setPingable(boolean pingable) {
        this.pingable = pingable;
    }

    public int getSQLCode() {
        return SQLCode;
    }

    public void setSQLCode(int SQLCode) {
        this.SQLCode = SQLCode;
    }

    public String getStatus() {
        return Status;
    }

    public void setStatus(String status) {
        Status = status;
    }

    public String getMessage() {
        return Message;
    }

    public void setMessage(String message) {
        Message = message;
    }

    public void addRetry(){
        this.maxRetry++;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public String getUIDApp() {
        return UIDApp;
    }

    public void setUIDApp(String UIDApp) {
        this.UIDApp = UIDApp;
    }

    public String getLastConnectTime() {
        if(this.LastConnectTime==null)
            return null;
        return this.LastConnectTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS"));
    }

    public void setLastConnectTime(LocalDateTime lastConnectTime) {
        LastConnectTime = lastConnectTime;
    }

    @Override
    public String toString() {
        return this.getDBName()+":"+this.getLDBID()+":"+this.getIP()+":"+this.getPort();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DB2InfoModel)) return false;

        DB2InfoModel that = (DB2InfoModel) o;

        if (id != that.id) return false;
        if (Port != that.Port) return false;
        if (LDBID != null ? !LDBID.equals(that.LDBID) : that.LDBID != null) return false;
        if (IP != null ? !IP.equals(that.IP) : that.IP != null) return false;
        if (DBName != null ? !DBName.equals(that.DBName) : that.DBName != null) return false;
        if (DBAlias != null ? !DBAlias.equals(that.DBAlias) : that.DBAlias != null) return false;
        if (User != null ? !User.equals(that.User) : that.User != null) return false;
        return Passwd != null ? Passwd.equals(that.Passwd) : that.Passwd == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (LDBID != null ? LDBID.hashCode() : 0);
        result = 31 * result + (IP != null ? IP.hashCode() : 0);
        result = 31 * result + Port;
        result = 31 * result + (DBName != null ? DBName.hashCode() : 0);
        result = 31 * result + (DBAlias != null ? DBAlias.hashCode() : 0);
        result = 31 * result + (User != null ? User.hashCode() : 0);
        result = 31 * result + (Passwd != null ? Passwd.hashCode() : 0);
        return result;
    }
}
