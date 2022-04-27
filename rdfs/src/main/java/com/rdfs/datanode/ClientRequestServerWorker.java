package com.rdfs.datanode;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;

public class ClientRequestServerWorker implements Runnable
{
    private int dataNodePort;
    private String dataPath;

    public ClientRequestServerWorker(int dataNodePort, String dataPath) {
        this.dataNodePort = dataNodePort;
        this.dataPath = dataPath;
    }

    @Override
    public void run() {
        try {
            ServerSocket socketServer = new ServerSocket(dataNodePort);
            while (true) {
                Socket clientSocket = socketServer.accept();
                Thread clientRequestThread = new Thread(new ClientRequestHandler(clientSocket, dataPath));
                clientRequestThread.start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}