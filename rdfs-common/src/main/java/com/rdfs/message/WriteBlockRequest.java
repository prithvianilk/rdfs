package com.rdfs.message;

import com.rdfs.NodeLocation;

public class WriteBlockRequest {
	public NodeLocation dataNodeLocations[];
	public byte block[];
	public String filename;
	public long blockNumber;

	public WriteBlockRequest(byte block[], NodeLocation dataNodeLocations[], String filename,
			long blockNumber) {
		this.block = block;
		this.dataNodeLocations = dataNodeLocations;
		this.filename = filename;
		this.blockNumber = blockNumber;
	}
}
