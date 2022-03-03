package com.rdfs.commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import com.rdfs.Constants;
import com.rdfs.Constants.MessageStatusCode;
import com.rdfs.NodeLocation;
import com.rdfs.SocketIOUtil;
import com.rdfs.messages.MessageType;
import com.rdfs.messages.WriteBlockRequestData;

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

	private File file;
	private SocketIOUtil nameNodeSocket;
	private FileInputStream fileInputStream;

	private void init() throws UnknownHostException, IOException {
		file = new File(filename);
		nameNodeSocket = new SocketIOUtil(new NodeLocation(nameNodeAddress, nameNodePort));
		fileInputStream = new FileInputStream(file.toPath().toString());
	}

	private void cleanUp() throws IOException {
		nameNodeSocket.close();
		fileInputStream.close();
	}

	@Override
	public void run() {
		try {
			init();
			sendAllCompleteBlocks(nameNodeSocket, fileInputStream);
			sendExtraBlockIfRemaining(nameNodeSocket, fileInputStream);
			cleanUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendExtraBlockIfRemaining(SocketIOUtil nameNodeSocket, FileInputStream fileInputStream) throws IOException, ClassNotFoundException, UnknownHostException {
		long fileLength = file.length();
		boolean extraBlockIsRemaining = fileLength % Constants.BLOCK_LENGTH != 0;
		if (extraBlockIsRemaining) {
			sendExtraBlock(nameNodeSocket, fileInputStream, fileLength);
		}
	}

	private void sendExtraBlock(SocketIOUtil nameNodeSocket, FileInputStream fileInputStream, long fileLength)
			throws IOException, ClassNotFoundException {
		long numOfCompleteBlocks = calcNumOfCompleteBlocks();
		long extraBlockLength = fileLength - Constants.BLOCK_LENGTH * numOfCompleteBlocks;
		long extraBlockNumber = numOfCompleteBlocks + 1;
		NodeLocation[] dataNodeLocations = getDataNodeLocations(nameNodeSocket);
		byte[] block = getBlock(fileInputStream, extraBlockLength);
		sendBlock(block, extraBlockNumber, dataNodeLocations);
	}

	private void sendAllCompleteBlocks(SocketIOUtil nameNodeSocket, FileInputStream fileInputStream) throws IOException, ClassNotFoundException, UnknownHostException {
		long numOfCompleteBlocks = calcNumOfCompleteBlocks();
		for (long blockNumber = 1; blockNumber <= numOfCompleteBlocks; ++blockNumber) {
			getLocationAndSendBlock(nameNodeSocket, fileInputStream, blockNumber);
		}
	}

	private void getLocationAndSendBlock(SocketIOUtil nameNodeSocket, FileInputStream fileInputStream, long blockNumber)
			throws IOException, ClassNotFoundException {
		NodeLocation[] dataNodeLocations = getDataNodeLocations(nameNodeSocket);
		byte[] block = getBlock(fileInputStream, Constants.BLOCK_LENGTH);
		sendBlock(block, blockNumber, dataNodeLocations);
	}

	private long calcNumOfCompleteBlocks() {
		long fileLength = file.length();
		long numOfCompleteBlocks = fileLength / Constants.BLOCK_LENGTH;
		return numOfCompleteBlocks;
	}

	private NodeLocation[] getDataNodeLocations(SocketIOUtil nameNodeSocket)
			throws IOException, ClassNotFoundException {
		nameNodeSocket.writeString(MessageType.GET_NEW_DATANODE_LOCATIONS_REQUEST.name());
		nameNodeSocket.flush();
		NodeLocation dataNodeLocations[] = (NodeLocation[]) nameNodeSocket.readObject();
		return dataNodeLocations;
	}

	private void sendBlock(byte block[], long blockNumber, NodeLocation[] dataNodeLocations) throws IOException {
		NodeLocation firstDataNodeLocation = dataNodeLocations[0];
		SocketIOUtil dataNodeSocket = new SocketIOUtil(firstDataNodeLocation);
		dataNodeSocket.writeString(MessageType.WRITE_BLOCK_REQUEST.name());
		dataNodeSocket.writeObject(new WriteBlockRequestData(block, dataNodeLocations, rdfsFilename, blockNumber));
		dataNodeSocket.flush();
		MessageStatusCode messageStatusCode = MessageStatusCode.valueOf(dataNodeSocket.readString());
		if (messageStatusCode == MessageStatusCode.ERROR) {
			// @todo
		}
		dataNodeSocket.close();
	}

	private byte[] getBlock(FileInputStream fileInputStream, long blockLength) throws IOException {
		byte block[] = new byte[(int) blockLength];
		fileInputStream.read(block);
		return block;
	}
}
