package com.rdfs.datanode;

import java.net.Socket;
import java.io.ObjectOutputStream;

import com.rdfs.NodeLocation;
import com.rdfs.Constants;

public class HeartbeatRequestWorker implements Runnable
{
    private NodeLocation dataNodeLocation;
    private String nameNodeAddress;
    private int nameNodePort;

    public HeartbeatRequestWorker(NodeLocation dataNodeLocation, String nameNodeAddress, int nameNodePort) {
        this.dataNodeLocation = dataNodeLocation;
        this.nameNodeAddress = nameNodeAddress;
        this.nameNodePort = nameNodePort;
    }

    @Override
    public void run()
    {
        try {
            while (true) {
                Socket socket = new Socket(nameNodeAddress, nameNodePort);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.writeObject(dataNodeLocation);
                outputStream.flush();
                Thread.sleep(Constants.DEFAULT_HEARTBEAT_THREAD_SLEEP_DURATION);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}