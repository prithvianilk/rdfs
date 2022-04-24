package com.rdfs.namenode;

import java.net.ServerSocket;
import java.net.Socket;

import com.rdfs.namenode.handler.Handler;
import com.rdfs.namenode.handler.HandlerFactory;
import com.rdfs.namenode.handler.HandlerType;

public class ServerWorker implements Runnable {
    private int port;
    private HandlerType handlerType;

    public ServerWorker(int port, HandlerType handlerType) {
        this.port = port;
        this.handlerType = handlerType;
    }

    @Override
    public void run() {
        try {
            ServerSocket socketServer = new ServerSocket(port);
            while (true) {
                Socket socket = socketServer.accept();
                Handler handler = HandlerFactory.createHandler(handlerType);
                handler.socket = socket;
                new Thread(handler).start();
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
