package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.rdfs.namenode.DataNodeHeartbeatStore;
import com.rdfs.namenode.DataNodeLocationStore;
import com.rdfs.NodeLocation;

public class HeartbeatHandler extends Handler {
    DataNodeHeartbeatStore heartbeatStore;
    DataNodeLocationStore locationStore;

    HeartbeatHandler() {
        heartbeatStore = DataNodeHeartbeatStore.getDataNodeHeartBeatStore();
        locationStore = DataNodeLocationStore.getDataNodeLocationStore();
    }

    @Override
    public void run() {
        try {
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
            NodeLocation dataNodeLocation = (NodeLocation) inputStream.readObject();
            System.out.println("Data Node " + dataNodeLocation + " sent a heartbeat.");
            heartbeatStore.updateHeartbeat(dataNodeLocation);
            locationStore.addDataNode(dataNodeLocation);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
