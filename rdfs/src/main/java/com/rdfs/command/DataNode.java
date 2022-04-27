package com.rdfs.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.InetAddress;

import com.rdfs.datanode.ClientRequestServerWorker;
import com.rdfs.datanode.HeartbeatRequestWorker;
import com.rdfs.NodeLocation;
import com.rdfs.Constants;

@Command(name = "datanode", description = "Start and configure a datanode on the rdfs cluster.")
public class DataNode implements Runnable {
    @Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-heartbeat-port" }, description = "Communication Port of the NameNode")
	private int nameNodeHeartbeatPort = Constants.DEFAULT_HEARTBEAT_PORT;

    @Option(names = { "--data-node-port" }, description = "Communication Port of the DataNode")
	private int dataNodePort = Constants.DEFAULT_DATA_NODE_PORT;

    @Override 
	public void run() {
        try {
            String currentIpAddress = InetAddress.getLocalHost().toString();
            Thread sendHeartBeatThread = new Thread(new HeartbeatRequestWorker(new NodeLocation(currentIpAddress, dataNodePort), nameNodeAddress, nameNodeHeartbeatPort));
            Thread clientThread = new Thread(new ClientRequestServerWorker(dataNodePort));
            sendHeartBeatThread.start();
            clientThread.start();
            sendHeartBeatThread.join();
            clientThread.join();
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}