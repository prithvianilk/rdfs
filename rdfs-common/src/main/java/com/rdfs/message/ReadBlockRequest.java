package com.rdfs.message;

import java.io.Serializable;

public class ReadBlockRequest implements Serializable {
	public String filename;
	public long blockNumber;

	public ReadBlockRequest(String filename, long blockNumber) {
		this.filename = filename;
		this.blockNumber = blockNumber;
	}
}
