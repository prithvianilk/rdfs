package com.rdfs.command;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;

import com.rdfs.BlockReplicasLocation;
import com.rdfs.Constants;
import com.rdfs.NodeLocation;
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
			HashSet<NodeLocation> dataNodeLocations = getDataNodeLocations();
			deleteBlocksInDataNodes(dataNodeLocations);
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void init() throws UnknownHostException, IOException {
		nameNodeSocket = new Socket(nameNodeAddress, nameNodePort);
	}

	private HashSet<NodeLocation> getDataNodeLocations()
			throws UnknownHostException, ClassNotFoundException, IOException {
		BlockReplicasLocation[] blockReplicasLocations = getBlockReplicaLocations();
		HashSet<NodeLocation> dataNodeLocations = groupBlockLocationsByDataNode(blockReplicasLocations);
		return dataNodeLocations;
	}

	private BlockReplicasLocation[] getBlockReplicaLocations()
			throws UnknownHostException, IOException, ClassNotFoundException {
		requestBlockLocations();
		BlockReplicasLocation[] blockReplicasLocations = readBlockLocations(nameNodeSocket);
		return blockReplicasLocations;
	}

	private void requestBlockLocations() throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(nameNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.GET_BLOCK_LOCATIONS_REQUEST.name());
		outputStream.writeUTF(rdfsFilename);
		outputStream.flush();
	}

	private BlockReplicasLocation[] readBlockLocations(Socket nameNodeSocket)
			throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(nameNodeSocket.getInputStream());
		BlockReplicasLocation[] blockReplicasLocations = (BlockReplicasLocation[]) inputStream.readObject();
		return blockReplicasLocations;
	}

	private HashSet<NodeLocation> groupBlockLocationsByDataNode(BlockReplicasLocation[] blockReplicasLocations) {
		HashSet<NodeLocation> dataNodeLocations = new HashSet<>();
		for (BlockReplicasLocation blockLocation : blockReplicasLocations) {
			addDataNodeLocations(dataNodeLocations, blockLocation);
		}
		return dataNodeLocations;
	}

	private void addDataNodeLocations(HashSet<NodeLocation> dataNodeLocations, BlockReplicasLocation blockLocation) {
		for (NodeLocation dataNodeLocation : blockLocation.dataNodeLocations) {
			dataNodeLocations.add(dataNodeLocation);
		}
	}

	private void deleteBlocksInDataNodes(HashSet<NodeLocation> dataNodeLocations) throws IOException {
		for (NodeLocation dataNodeLocation : dataNodeLocations) {
			deleteBlocksInSingleDataNode(dataNodeLocation);
		}
	}

	private void deleteBlocksInSingleDataNode(NodeLocation dataNodeLocation) throws IOException {
		Socket dataNodeSocket = new Socket(dataNodeLocation.address, dataNodeLocation.port);
		sendDeleteRequest(dataNodeSocket);
		dataNodeSocket.close();
	}

	private void sendDeleteRequest(Socket dataNodeSocket) throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(dataNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.DELETE_BLOCK_REQUEST.name());
		outputStream.writeUTF(rdfsFilename);
		outputStream.flush();
	}

	void cleanUp() throws IOException {
		nameNodeSocket.close();
	}
}
