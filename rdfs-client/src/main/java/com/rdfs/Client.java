package com.rdfs;

import com.rdfs.command.Delete;
import com.rdfs.command.Read;
import com.rdfs.command.Write;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "rdfs-client ", subcommands = { Read.class, Write.class,
		Delete.class }, description = "A cli client to access rdfs.")

public class Client {
	public static void main(String[] args) {
		int exitCode = new CommandLine(new Client()).execute(args);
		System.exit(exitCode);
	}
}