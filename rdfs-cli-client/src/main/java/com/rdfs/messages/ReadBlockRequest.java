package com.rdfs.messages;

public class ReadBlockRequest {
	private String filename;
	private long blockNumber;

	public ReadBlockRequest(String filename, long blockNumber) {
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
