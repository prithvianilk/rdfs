package com.rdfs.datanode;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;

public class ClientRequestServerWorker implements Runnable
{
    private int dataNodePort;

    public ClientRequestServerWorker(int dataNodePort) {
        this.dataNodePort = dataNodePort;
    }

    @Override
    public void run() {
        try {
            ServerSocket socketServer = new ServerSocket(dataNodePort);
            while (true) {
                Socket clientSocket = socketServer.accept();
                Thread clientRequestThread = new Thread(new ClientRequestHandler(clientSocket));
                clientRequestThread.start();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}