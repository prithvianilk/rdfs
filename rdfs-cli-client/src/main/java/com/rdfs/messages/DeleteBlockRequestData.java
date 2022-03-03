package com.rdfs.messages;

import com.rdfs.NodeLocation;

public class DeleteBlockRequestData {
	private NodeLocation dataNodeLocation;
	private String filename;

	public DeleteBlockRequestData(NodeLocation dataNodeLocation, String filename) {
		this.dataNodeLocation = dataNodeLocation;
		this.filename = filename;
	}

	public String getFilename() {
		return filename;
	}

	public NodeLocation getDataNodeLocation() {
		return dataNodeLocation;
	}
}
