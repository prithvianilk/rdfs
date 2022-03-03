package com.rdfs;

import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SocketIOUtil {
	Socket socket;
	ObjectInputStream inputStream;
	ObjectOutputStream outputStream;

	public SocketIOUtil(NodeLocation NodeLocation) throws UnknownHostException, IOException {
		socket = new Socket(NodeLocation.getAddress(), NodeLocation.getPort());
		outputStream = new ObjectOutputStream(socket.getOutputStream());
		inputStream = new ObjectInputStream(socket.getInputStream());
	}

	public void writeString(String string) throws IOException {
		outputStream.writeUTF(string);
	}

	public void writeObject(Object object) throws IOException {
		outputStream.writeObject(object);
	}

	public void flush() throws IOException {
		outputStream.flush();
	}

	public Object readObject() throws ClassNotFoundException, IOException {
		return inputStream.readObject();
	}

	public String readString() throws IOException {
		return inputStream.readUTF();
	}

	public void close() throws IOException {
		socket.close();
	}
}
