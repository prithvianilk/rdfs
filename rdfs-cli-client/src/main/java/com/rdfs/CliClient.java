package com.rdfs;

import com.rdfs.commands.Delete;
import com.rdfs.commands.Read;
import com.rdfs.commands.Write;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "rdfs-client ", subcommands = { Read.class, Write.class,
		Delete.class }, description = "A Cli Client to access rdfs.")

public class CliClient {
	public static void main(String[] args) {
		int exitCode = new CommandLine(new CliClient()).execute(args);
		System.exit(exitCode);
	}
}