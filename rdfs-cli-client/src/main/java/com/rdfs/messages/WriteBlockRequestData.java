package com.rdfs.messages;

import com.rdfs.NodeLocation;

public class WriteBlockRequestData {
	private NodeLocation dataNodeLocations[];
	private byte block[];
	private String filename;
	private long blockNumber;

	public WriteBlockRequestData(byte block[], NodeLocation dataNodeLocations[], String filename,
			long blockNumber) {
		this.block = block;
		this.dataNodeLocations = dataNodeLocations;
		this.filename = filename;
		this.blockNumber = blockNumber;
	}

	public byte[] getBlock() {
		return block;
	}

	public long getBlockNumber() {
		return blockNumber;
	}

	public String getFilename() {
		return filename;
	}

	public NodeLocation[] getDataNodeLocations() {
		return dataNodeLocations;
	}
}
