package com.rdfs.commands;

import picocli.CommandLine.Command;

@Command(name = "datanode", description = "Start and configure a datanode on the rdfs cluster.")
public class DataNode implements Runnable {
    @Override 
	public void run() {
    }
}
