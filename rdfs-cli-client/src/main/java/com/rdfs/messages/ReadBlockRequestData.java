package com.rdfs.messages;

public class ReadBlockRequestData {
	private String filename;
	private long blockNumber;

	public ReadBlockRequestData(String filename, long blockNumber) {
		this.filename = filename;
		this.blockNumber = blockNumber;
	}

	public String getFilename() {
		return filename;
	}

	public long getBlockNumber() {
		return blockNumber;
	}
}
