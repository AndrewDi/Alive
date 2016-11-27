package com.andrew;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by Andrew on 27/11/2016.
 */
public class MemoryUsage {
    public static SortedMap<String,Object> DisplayMemory(){
        SortedMap<String,Object> maps = new TreeMap<>();
        maps.put("CurrentTimeStamp", LocalDateTime.now().toString());
        DecimalFormat df = new DecimalFormat("0.00") ;
        long totalMem = Runtime.getRuntime().totalMemory();

        maps.put("TotalMemory",df.format(totalMem/1024.0/1024.0) + " MB");

        long maxMem = Runtime.getRuntime().maxMemory();
        maps.put("MaxMemory",df.format(maxMem/1024.0/1024.0) + " MB");

        long freeMem = Runtime.getRuntime().freeMemory();
        maps.put("FreeMemory",df.format(freeMem/1024.0/1024.0) + " MB");

        long usedMem = totalMem-freeMem;
        maps.put("UsedMemory",df.format(usedMem/1024.0/1024.0) + " MB");

        return maps;
    }
}
