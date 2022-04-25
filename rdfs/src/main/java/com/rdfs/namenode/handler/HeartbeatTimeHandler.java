package com.rdfs.namenode.handler;

import java.util.HashMap;
import java.util.Map;

import com.rdfs.Constants;
import com.rdfs.namenode.DataNodeHeartbeatStore;
import com.rdfs.NodeLocation;

public class HeartbeatTimeHandler extends Handler {
    @Override
    public void run() {
        try {
            //TODO prevent race condition
            //TODO make itearble or get iterator and access
            HashMap<NodeLocation, Long> dataNodeLastHeartBeatMap = DataNodeHeartbeatStore.getDataNodeHeartBeatStore().lastHeartbeatMap;
            while (true) {
                for (var mapEntry: dataNodeLastHeartBeatMap.entrySet()) {
                    var currentTime = System.currentTimeMillis();  
                    var lastHeartbeatTime = (long) mapEntry.getValue();
                    if (currentTime - lastHeartbeatTime > Constants.DEFAULT_HEARTBEAT_THRESHOLD) {
                        //TODO mark node as unavailable
                    }
                }
                Thread.sleep(Constants.DEFAULT_HEARTBEAT_THREAD_SLEEP_DURATION);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}