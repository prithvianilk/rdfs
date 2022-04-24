package com.rdfs;

public class NodeLocation {
	public String address;
	public int port;

	public NodeLocation(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public String toString() {
		return address + ":" + port;
	}
}
