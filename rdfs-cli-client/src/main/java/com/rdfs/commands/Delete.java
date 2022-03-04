package com.rdfs.commands;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;

import com.rdfs.BlockReplicasLocation;
import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.SocketIOUtil;
import com.rdfs.messages.MessageType;

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

	@Override
	public void run() {
		try {
			HashSet<NodeLocation> dataNodeLocations = getDataNodeLocations();
			deleteBlocksInDataNodes(dataNodeLocations);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private HashSet<NodeLocation> getDataNodeLocations()
			throws UnknownHostException, ClassNotFoundException, IOException {
		BlockReplicasLocation[] blockReplicasLocations = getBlockReplicaLocations();
		HashSet<NodeLocation> dataNodeLocations = groupBlockLocationsByDataNode(blockReplicasLocations);
		return dataNodeLocations;
	}

	private BlockReplicasLocation[] getBlockReplicaLocations()
			throws UnknownHostException, IOException, ClassNotFoundException {
		SocketIOUtil nameNodeSocket = new SocketIOUtil(new NodeLocation(nameNodeAddress, nameNodePort));
		nameNodeSocket.writeString(MessageType.GET_DATANODE_LOCATIONS_REQUEST.name());
		nameNodeSocket.writeString(rdfsFilename);
		nameNodeSocket.flush();
		BlockReplicasLocation[] blockReplicasLocations = (BlockReplicasLocation[]) nameNodeSocket.readObject();
		nameNodeSocket.close();
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
		for (NodeLocation dataNodeLocation : blockLocation.getDataNodeLocations()) {
			dataNodeLocations.add(dataNodeLocation);
		}
	}

	private void deleteBlocksInDataNodes(HashSet<NodeLocation> dataNodeLocations) throws IOException {
		for (NodeLocation dataNodeLocation : dataNodeLocations) {
			deleteBlocksInSingleDataNode(dataNodeLocation);
		}
	}

	private void deleteBlocksInSingleDataNode(NodeLocation dataNodeLocation) throws IOException {
		SocketIOUtil dataNodeSocket = new SocketIOUtil(dataNodeLocation);
		dataNodeSocket.writeString(MessageType.DELETE_BLOCK_REQUEST.name());
		dataNodeSocket.writeString(rdfsFilename);
		dataNodeSocket.flush();
		dataNodeSocket.close();
	}
}
