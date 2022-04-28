package com.rdfs.namenode;

import java.util.HashMap;

import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;

import com.rdfs.NodeLocation;

public class DataNodeHeartbeatStore {
    private static DataNodeHeartbeatStore store = null;
    private RMap<String, Long> lastHeartbeatMap;

    private DataNodeHeartbeatStore() {
        RedissonClient redisClient = RedisSingleton.getRedis().getClient();
        lastHeartbeatMap = redisClient.getMap("last-heartbeat-map");
    }

    public static DataNodeHeartbeatStore getDataNodeHeartBeatStore() {
        if (store == null) {
            store = new DataNodeHeartbeatStore();
        }
        return store;
    }

    public void updateHeartbeat(NodeLocation dataNodeLocation) {
        long currentTime = System.currentTimeMillis();  
        lastHeartbeatMap.put(dataNodeLocation.toString(), currentTime);
    }

    public RMap<String, Long> getHeartbeatMap() {
        return lastHeartbeatMap;
    }
}
