package com.rdfs;

import com.rdfs.command.NameNode;
import com.rdfs.command.DataNode;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "rdfs", subcommands = { NameNode.class, DataNode.class }, description = "A cli to start and configure nodes on rdfs.")
public class Rdfs {
	public static void main(String[] args) {
		int exitCode = new CommandLine(new Rdfs()).execute(args);
		System.exit(exitCode);
	}
}