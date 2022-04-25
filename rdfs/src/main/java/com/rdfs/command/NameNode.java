package com.rdfs.command;

import com.rdfs.namenode.ServerWorker;
import com.rdfs.namenode.handler.HandlerType;
import com.rdfs.namenode.handler.HeartbeatTimeHandler;
import com.rdfs.Constants;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "namenode", description = "Start and configure a namenode on the rdfs cluster.")
public class NameNode implements Runnable {
	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

	@Option(names = { "--heartbeat-port" }, description = "Heartbeat Port of the NameNode")
	private int heartbeatPort = Constants.DEFAULT_HEARTBEAT_PORT;

    @Override 
	public void run() {
		try {
			var heartbeatWorker = new ServerWorker(heartbeatPort, HandlerType.HEARTBEAT);
			var nameNodeWorker = new ServerWorker(nameNodePort, HandlerType.CLIENT);
			var heartbeatThread = new Thread(heartbeatWorker);
			var nameNodeThread = new Thread(nameNodeWorker);
			var heartbeatTimeThread = new Thread(new HeartbeatTimeHandler());
			heartbeatThread.start();
			nameNodeThread.start();
			heartbeatTimeThread.start();
			heartbeatThread.join();
			nameNodeThread.join();
			heartbeatTimeThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}
