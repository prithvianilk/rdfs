package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.rdfs.namenode.DataNodeHeartbeatStore;
import com.rdfs.NodeLocation;

public class HeartbeatHandler extends Handler {
    @Override
    public void run() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
            NodeLocation dataNodeLocation = (NodeLocation) inputStream.readObject();
            var heartbeatStore = DataNodeHeartbeatStore.getDataNodeHeartBeatStore();
            System.out.println("Data Node " + dataNodeLocation + " sent a heartbeat.");
            heartbeatStore.updateHeartbeat(dataNodeLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
