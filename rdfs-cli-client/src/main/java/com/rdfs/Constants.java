package com.rdfs;

public class Constants {
	public static final long BLOCK_LENGTH;
	public static final String DEFAULT_NAME_NODE_ADDRESS;
	public static final int DEFAULT_NAME_NODE_PORT;

	static {
		BLOCK_LENGTH = (long) 128e6;
		DEFAULT_NAME_NODE_ADDRESS = "0.0.0.0";
		DEFAULT_NAME_NODE_PORT = 3530;
	}
}
