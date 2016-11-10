package com.andrew;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Andrew on 27/10/2016.
 */
public class DB2InfoList {
    private ConcurrentHashMap<String,DB2InfoModel> db2List = new ConcurrentHashMap<String, DB2InfoModel>();

    public void AddDB2Info(DB2InfoModel db2InfoModel){
        this.db2List.put(db2InfoModel.toString(),db2InfoModel);
    }

    public void ReplaceDB2Info(DB2InfoModel db2InfoModel){
        this.db2List.replace(db2InfoModel.toString(),db2InfoModel);
    }

    public void RemoveDB2Info(String key){
        this.db2List.remove(key);
    }

    public void RemoveDB2Info(DB2InfoModel db2InfoModel){
        this.db2List.remove(db2InfoModel.toString());
    }

    public ConcurrentHashMap<String,DB2InfoModel> getDb2List(){
        return db2List;
    }

    public DB2InfoModel getDB2Info(String key){
        return this.db2List.get(key);
    }
}
