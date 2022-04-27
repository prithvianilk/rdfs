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
            int id = inputStream.readInt();
            var heartbeatStore = DataNodeHeartbeatStore.getDataNodeHeartBeatStore();
            heartbeatStore.updateHeartbeat(id);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
