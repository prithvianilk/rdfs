package com.rdfs.commands;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.SocketIOUtil;
import com.rdfs.messages.MessageType;
import com.rdfs.messages.ReadBlockRequestData;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "read", description = "Read a file from rdfs")
public class Read implements Runnable {
	@Parameters(index = "0", description = "The local path of the file to write.")
	private String filename;

	@Parameters(index = "1", description = "The filename of the file on rdfs.")
	private String rdfsFilename;

	@Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

	private SocketIOUtil nameNodeSocket;
	private FileOutputStream fileOutputStream;

	@Override
	public void run() {
		try {
			init();
			NodeLocation[] dataNodeLocations = getDataNodeLocations();
			readAllBlocks(dataNodeLocations);
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void init() throws UnknownHostException, IOException {
		fileOutputStream = new FileOutputStream(filename);
		nameNodeSocket = new SocketIOUtil(new NodeLocation(nameNodeAddress, nameNodePort));
	}

	private void cleanUp() throws IOException {
		fileOutputStream.close();
		nameNodeSocket.close();
	}

	private void readAllBlocks(NodeLocation[] dataNodeLocations)
			throws UnknownHostException, IOException, ClassNotFoundException {
		int blockNumber = 1;
		for (NodeLocation dataNodeLocation : dataNodeLocations) {
			readBlkAndWriteToFile(blockNumber, dataNodeLocation);
			++blockNumber;
		}
	}

	private void readBlkAndWriteToFile(int blockNumber, NodeLocation dataNodeLocation)
			throws UnknownHostException, IOException, ClassNotFoundException {
		SocketIOUtil dataNodeSocket = new SocketIOUtil(dataNodeLocation);
		byte[] blockContents = getBlockContents(blockNumber, dataNodeSocket);
		writeBlockAndCleanUp(blockContents, dataNodeSocket);
	}

	private void writeBlockAndCleanUp(byte[] blockContents, SocketIOUtil dataNodeSocket) throws IOException {
		fileOutputStream.write(blockContents);
		dataNodeSocket.close();
	}

	private byte[] getBlockContents(int blockNumber, SocketIOUtil dataNodeSocket)
			throws IOException, ClassNotFoundException {
		dataNodeSocket.writeString(MessageType.READ_BLOCK_REQUEST.name());
		dataNodeSocket.writeObject(new ReadBlockRequestData(rdfsFilename, blockNumber));
		dataNodeSocket.flush();
		byte[] blockContents = (byte[]) dataNodeSocket.readObject();
		return blockContents;
	}

	private NodeLocation[] getDataNodeLocations() throws IOException, ClassNotFoundException {
		nameNodeSocket.writeString(MessageType.GET_FILE_LOCATION_REQUEST.name());
		nameNodeSocket.flush();
		NodeLocation dataNodeLocations[] = (NodeLocation[]) nameNodeSocket.readObject();
		return dataNodeLocations;
	}
}
