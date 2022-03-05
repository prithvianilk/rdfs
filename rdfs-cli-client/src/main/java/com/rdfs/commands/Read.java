package com.rdfs.commands;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.SocketIOUtil;
import com.rdfs.messages.MessageType;
import com.rdfs.messages.ReadBlockRequest;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "read", description = "Read a file from rdfs and write it's contents to a local file")
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

	private NodeLocation[] getDataNodeLocations() throws IOException, ClassNotFoundException {
		nameNodeSocket.writeString(MessageType.GET_FILE_LOCATION_REQUEST.name());
		nameNodeSocket.flush();
		NodeLocation dataNodeLocations[] = (NodeLocation[]) nameNodeSocket.readObject();
		return dataNodeLocations;
	}

	private void readAllBlocks(NodeLocation[] dataNodeLocations)
			throws UnknownHostException, IOException, ClassNotFoundException {
		int blockNumber = 1;
		for (NodeLocation dataNodeLocation : dataNodeLocations) {
			readBlkAndWriteToFile(dataNodeLocation, blockNumber);
			++blockNumber;
		}
	}

	private void readBlkAndWriteToFile(NodeLocation dataNodeLocation, int blockNumber)
			throws UnknownHostException, IOException, ClassNotFoundException {
		SocketIOUtil dataNodeSocket = new SocketIOUtil(dataNodeLocation);
		requestBlockContent(dataNodeSocket, blockNumber);
		byte[] blockContents = getBlockAndCleanUp(dataNodeSocket, blockNumber);
		fileOutputStream.write(blockContents);
	}

	private void requestBlockContent(SocketIOUtil dataNodeSocket, int blockNumber) throws IOException {
		dataNodeSocket.writeString(MessageType.READ_BLOCK_REQUEST.name());
		dataNodeSocket.writeObject(new ReadBlockRequest(rdfsFilename, blockNumber));
		dataNodeSocket.flush();
	}

	private byte[] getBlockAndCleanUp(SocketIOUtil dataNodeSocket, int blockNumber)
			throws IOException, ClassNotFoundException {
		byte[] blockContents = (byte[]) dataNodeSocket.readObject();
		dataNodeSocket.close();
		return blockContents;
	}

	private void cleanUp() throws IOException {
		fileOutputStream.close();
		nameNodeSocket.close();
	}
}
