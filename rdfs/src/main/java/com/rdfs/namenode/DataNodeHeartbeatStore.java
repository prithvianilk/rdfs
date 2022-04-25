package com.rdfs.namenode;

import java.util.HashMap;
import com.rdfs.NodeLocation;

public class DataNodeHeartbeatStore {
    private static DataNodeHeartbeatStore store = null;
    public HashMap<NodeLocation, Long> lastHeartbeatMap;

    private DataNodeHeartbeatStore() {
        lastHeartbeatMap = new HashMap<>();
    }

    public static DataNodeHeartbeatStore getDataNodeHeartBeatStore() {
        if (store == null) {
            store = new DataNodeHeartbeatStore();
        }
        return store;
    }

    public void updateHeartbeat(NodeLocation nodeLocation) {
        long currentTime = System.currentTimeMillis();  
        lastHeartbeatMap.put(nodeLocation, currentTime);
    }
}
