package com.rdfs.commands;

import picocli.CommandLine.Command;

@Command(name = "namenode", description = "Start and configure a namenode on the rdfs cluster.")
public class NameNode implements Runnable {
    @Override 
	public void run() {
    }
}
