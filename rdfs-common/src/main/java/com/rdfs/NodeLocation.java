package com.rdfs;

import java.io.Serializable;

public class NodeLocation implements Serializable {

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
