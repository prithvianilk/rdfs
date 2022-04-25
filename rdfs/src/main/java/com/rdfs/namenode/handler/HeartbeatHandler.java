package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.rdfs.namenode.DataNodeHeartbeatStore;
import com.rdfs.NodeLocation;

public class HeartbeatHandler extends Handler {
    @Override
    public void run() {
        var address = socket.getInetAddress().toString();
        var port = socket.getPort();
        var heartbeatStore = DataNodeHeartbeatStore.getDataNodeHeartBeatStore();
        heartbeatStore.updateHeartbeat(new NodeLocation(address, port));
    }
}
