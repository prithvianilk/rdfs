package com.rdfs.message;

import java.io.Serializable;

public class GetNewDataNodeLocationsRequest implements Serializable {
    public String filename;
	public boolean isNewWrite;

    public GetNewDataNodeLocationsRequest(String filename) {
        this.filename = filename;
		this.isNewWrite = true;
    }
}
