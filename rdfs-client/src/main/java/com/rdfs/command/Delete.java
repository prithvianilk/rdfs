package com.rdfs.command;

import com.rdfs.Constants;
import com.rdfs.operation.DeleteFileHandler;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "delete", description = "Delete a file from rdfs.")
public class Delete implements Runnable {
	@Parameters(index = "0", description = "The filename of the file on rdfs.")
	private String rdfsFilename;

	@Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

	@Override
	public void run() {
		try {
			DeleteFileHandler deleteFileOperation = new DeleteFileHandler(rdfsFilename, nameNodeAddress,
					nameNodePort);
			deleteFileOperation.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
