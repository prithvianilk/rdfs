package com.rdfs.message;

public class ReadBlockRequest {
	public String filename;
	public long blockNumber;

	public ReadBlockRequest(String filename, long blockNumber) {
		this.filename = filename;
		this.blockNumber = blockNumber;
	}
}
