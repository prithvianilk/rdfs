package com.rdfs;

public class NodeLocation {
	private String address;
	private int port;

	public NodeLocation(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		return address + ":" + port;
	}
}