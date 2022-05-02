package com.rdfs.message;

import java.io.Serializable;

import com.rdfs.NodeLocation;

public class DeleteBlockRequest implements Serializable {
	public String filename;
	public NodeLocation[] dataNodeLocations;

	public DeleteBlockRequest(String filename, NodeLocation[] dataNodeLocations) {
		this.filename = filename;
        this.dataNodeLocations = dataNodeLocations;
	}
}
