package com.rdfs.command;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.message.DeleteBlockRequest;
import com.rdfs.message.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "delete", description = "Delete a file from rdfs.")
public class Delete implements Runnable {
	@Parameters(index = "0", description = "The filename of the file on rdfs.")
	private String rdfsFilename;

	@Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

	private Socket nameNodeSocket;

	@Override
	public void run() {
		try {
			init();
			NodeLocation[] dataNodeLocations = getDataNodeLocations();
			deleteBlocksInDataNodes(dataNodeLocations);
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void init() throws UnknownHostException, IOException {
		nameNodeSocket = new Socket(nameNodeAddress, nameNodePort);
	}

	private NodeLocation[] getDataNodeLocations()
			throws UnknownHostException, ClassNotFoundException, IOException {
		requestDataNodeLocations();
		NodeLocation[] dataNodeLocations = readDataNodeLocations(nameNodeSocket);
		return dataNodeLocations;
	}

	private void requestDataNodeLocations() throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(nameNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.GET_BLOCK_LOCATIONS_REQUEST.name());
		outputStream.writeUTF(rdfsFilename);
		outputStream.flush();
	}

	private NodeLocation[] readDataNodeLocations(Socket nameNodeSocket)
			throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(nameNodeSocket.getInputStream());
		NodeLocation[] dataNodeLocations = (NodeLocation []) inputStream.readObject();
		return dataNodeLocations;
	}

	private void deleteBlocksInDataNodes(NodeLocation[] dataNodeLocations) throws IOException {
		NodeLocation firstNodeLocation = dataNodeLocations[0];
		Socket dataNodeSocket = new Socket(firstNodeLocation.address, firstNodeLocation.port);
		sendDeleteRequest(dataNodeSocket, dataNodeLocations);
		dataNodeSocket.close();
	}

	private void sendDeleteRequest(Socket dataNodeSocket, NodeLocation[] dataNodeLocations) throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(dataNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.DELETE_BLOCK_REQUEST.name());
		outputStream.writeObject(new DeleteBlockRequest(rdfsFilename, dataNodeLocations));
		outputStream.flush();
	}

	void cleanUp() throws IOException {
		nameNodeSocket.close();
	}
}
