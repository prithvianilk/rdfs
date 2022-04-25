package com.rdfs.command;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.message.MessageType;
import com.rdfs.message.ReadBlockRequest;

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

	private Socket nameNodeSocket;
	private FileOutputStream fileOutputStream;

	@Override
	public void run() {
		try {
			init();
			NodeLocation[] dataNodeLocations = getDataNodeLocations();
			readAllBlocksAndWriteToFile(dataNodeLocations);
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void init() throws IOException {
		nameNodeSocket = new Socket(nameNodeAddress, nameNodePort);
		fileOutputStream = new FileOutputStream(filename);
	}

	private NodeLocation[] getDataNodeLocations() throws IOException, ClassNotFoundException {
		requestDataNodeLocations();
		NodeLocation[] dataNodeLocations = readDataNodeLocations();
		return dataNodeLocations;
	}

	private void requestDataNodeLocations() throws IOException {
		ObjectOutputStream nameNodeOutputStream = new ObjectOutputStream(nameNodeSocket.getOutputStream());
		nameNodeOutputStream.writeUTF(MessageType.GET_DATANODE_LOCATIONS_REQUEST.name());
		nameNodeOutputStream.writeUTF(rdfsFilename);
		nameNodeOutputStream.flush();
	}

	private NodeLocation[] readDataNodeLocations() throws IOException, ClassNotFoundException {
		ObjectInputStream nameNodeInputStream = new ObjectInputStream(nameNodeSocket.getInputStream());
		NodeLocation dataNodeLocations[] = (NodeLocation[]) nameNodeInputStream.readObject();
		return dataNodeLocations;
	}

	private void readAllBlocksAndWriteToFile(NodeLocation[] dataNodeLocations)
			throws IOException, ClassNotFoundException {
		int blockNumber = 1;
		for (NodeLocation dataNodeLocation : dataNodeLocations) {
			readBlockAndWriteToFile(dataNodeLocation, blockNumber);
			++blockNumber;
		}
	}

	private void readBlockAndWriteToFile(NodeLocation dataNodeLocation, int blockNumber)
			throws IOException, ClassNotFoundException {
		Socket dataNodeSocket = new Socket(dataNodeLocation.address, dataNodeLocation.port);
		requestBlockContent(dataNodeSocket, blockNumber);
		byte[] blockContents = readBlock(dataNodeSocket);
		dataNodeSocket.close();
		fileOutputStream.write(blockContents);
	}

	private void requestBlockContent(Socket dataNodeSocket, int blockNumber) throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(dataNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.READ_BLOCK_REQUEST.name());
		outputStream.writeObject(new ReadBlockRequest(rdfsFilename, blockNumber));
		outputStream.flush();
	}

	private byte[] readBlock(Socket dataNodeSocket)
			throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(dataNodeSocket.getInputStream());
		byte[] blockContents = (byte[]) inputStream.readObject();
		return blockContents;
	}

	void cleanUp() throws IOException {
		nameNodeSocket.close();
		fileOutputStream.close();
	}
}
