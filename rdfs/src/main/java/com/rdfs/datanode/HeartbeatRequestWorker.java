package com.rdfs.datanode;

import java.net.Socket;
import java.io.ObjectOutputStream;
import com.rdfs.Constants;

public class HeartbeatRequestWorker implements Runnable
{
    private int dataNodeId;
    private String nameNodeAddress;
    private int nameNodePort;

    public HeartbeatRequestWorker(int dataNodeId, String nameNodeAddress, int nameNodePort) {
        this.dataNodeId = dataNodeId;
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
                outputStream.writeInt(dataNodeId);
                outputStream.flush();
                Thread.sleep(Constants.DEFAULT_HEARTBEAT_THREAD_SLEEP_DURATION);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}