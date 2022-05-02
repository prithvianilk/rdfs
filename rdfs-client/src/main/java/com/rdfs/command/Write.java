package com.rdfs.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.message.GetNewDataNodeLocationsRequest;
import com.rdfs.message.MessageType;
import com.rdfs.message.WriteBlockRequest;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "write", description = "Write a local file to rdfs.")
public class Write implements Runnable {
	@Parameters(index = "0", description = "The local path of the file to write.")
	private String filename;

	@Parameters(index = "1", description = "The filename of the file on rdfs.")
	private String rdfsFilename;

	@Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

	@Option(names = { "--block-size" }, description = "Block Size a file is split up into.")
	private long configBlockSize = Constants.DEFAULT_BLOCK_SIZE;

	private File file;
	private FileInputStream fileInputStream;
	private Socket nameNodeSocket;
	private GetNewDataNodeLocationsRequest getDataNodeLocationRequest;

	@Override
	public void run() {
		try {
			init();
			sendAllCompleteBlocks();
			sendExtraBlockIfRemaining();
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void init() throws IOException {
		file = new File(filename);
		fileInputStream = new FileInputStream(file.toPath().toString());
		file = new File(filename);
		getDataNodeLocationRequest = new GetNewDataNodeLocationsRequest(rdfsFilename);
	}

	private void sendAllCompleteBlocks() throws IOException, ClassNotFoundException {
		long numOfCompleteBlocks = calcNumOfCompleteBlocks();
		for (long blockNumber = 1; blockNumber <= numOfCompleteBlocks; ++blockNumber) {
			getLocationAndSendBlock(blockNumber);
		}
	}

	private long calcNumOfCompleteBlocks() {
		long fileLength = file.length();
		long numOfCompleteBlocks = fileLength / configBlockSize;
		return numOfCompleteBlocks;
	}

	private void getLocationAndSendBlock(long blockNumber)
			throws IOException, ClassNotFoundException {
		NodeLocation[] dataNodeLocations = getDataNodeLocations();
		byte[] block = readBlock(configBlockSize);
		sendBlock(block, blockNumber, dataNodeLocations);
	}

	private NodeLocation[] getDataNodeLocations()
			throws IOException, ClassNotFoundException {
		requestDataNodeLocations();
		getDataNodeLocationRequest.isNewWrite = false;
		NodeLocation[] dataNodeLocations = readDataNodeLocations();
		return dataNodeLocations;
	}

	private void requestDataNodeLocations() throws IOException {
		nameNodeSocket = new Socket(nameNodeAddress, nameNodePort);
		ObjectOutputStream outputStream = new ObjectOutputStream(nameNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.GET_NEW_DATANODE_LOCATIONS_REQUEST.name());
		outputStream.writeObject(getDataNodeLocationRequest);
		outputStream.flush();
	}

	private NodeLocation[] readDataNodeLocations() throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream(nameNodeSocket.getInputStream());
		NodeLocation dataNodeLocations[] = (NodeLocation[]) inputStream.readObject();
		return dataNodeLocations;
	}

	private byte[] readBlock(long blockSize) throws IOException {
		byte block[] = new byte[(int) blockSize];
		fileInputStream.read(block);
		return block;
	}

	private void sendBlock(byte block[], long blockNumber, NodeLocation[] dataNodeLocations) throws IOException {
		NodeLocation firstLocation = dataNodeLocations[0];
		Socket dataNodeSocket = new Socket(firstLocation.address, firstLocation.port);
		writeBlock(block, blockNumber, dataNodeLocations, dataNodeSocket);
		dataNodeSocket.close();
	}

	private void writeBlock(byte[] block, long blockNumber, NodeLocation[] dataNodeLocations, Socket dataNodeSocket)
			throws IOException {
		ObjectOutputStream outputStream = new ObjectOutputStream(dataNodeSocket.getOutputStream());
		outputStream.writeUTF(MessageType.WRITE_BLOCK_REQUEST.name());
		outputStream.writeObject(new WriteBlockRequest(block, dataNodeLocations, rdfsFilename, blockNumber));
		outputStream.flush();
	}

	private void sendExtraBlockIfRemaining() throws IOException, ClassNotFoundException {
		long fileLength = file.length();
		boolean extraBlockIsRemaining = fileLength % configBlockSize != 0;
		if (extraBlockIsRemaining) {
			sendExtraBlock(fileLength);
		}
	}

	private void sendExtraBlock(long fileLength)
			throws IOException, ClassNotFoundException {
		long numOfCompleteBlocks = calcNumOfCompleteBlocks();
		long extraBlockLength = fileLength - configBlockSize * numOfCompleteBlocks;
		long extraBlockNumber = numOfCompleteBlocks + 1;
		NodeLocation[] dataNodeLocations = getDataNodeLocations();
		byte[] block = readBlock(extraBlockLength);
		sendBlock(block, extraBlockNumber, dataNodeLocations);
	}

	private void cleanUp() throws IOException {
		nameNodeSocket.close();
		fileInputStream.close();
	}
}
