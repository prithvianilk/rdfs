package com.rdfs.operation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import com.rdfs.NodeLocation;
import com.rdfs.message.DeleteBlockRequest;
import com.rdfs.message.MessageType;

public class DeleteFileHandler {
    private String rdfsFilename;
    private Socket nameNodeSocket;

    public DeleteFileHandler(String rdfsFilename, String nameNodeAddress, int nameNodePort)
            throws UnknownHostException, IOException {
        this.rdfsFilename = rdfsFilename;
        nameNodeSocket = new Socket(nameNodeAddress, nameNodePort);
    }

    public void start() {
        try {
            NodeLocation[] dataNodeLocations = getDataNodeLocations();
            deleteBlocksInDataNodes(dataNodeLocations);
            cleanUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private NodeLocation[] getDataNodeLocations()
            throws UnknownHostException, ClassNotFoundException, IOException {
        requestDataNodeLocations();
        NodeLocation[] dataNodeLocations = readDataNodeLocations(nameNodeSocket);
        return dataNodeLocations;
    }

    private void requestDataNodeLocations() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(nameNodeSocket.getOutputStream());
        outputStream.writeUTF(MessageType.GET_BLOCK_LOCATIONS_REQUEST.name());
        outputStream.writeUTF(rdfsFilename);
        outputStream.flush();
    }

    private NodeLocation[] readDataNodeLocations(Socket nameNodeSocket)
            throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream = new ObjectInputStream(nameNodeSocket.getInputStream());
        NodeLocation[] dataNodeLocations = (NodeLocation[]) inputStream.readObject();
        return dataNodeLocations;
    }

    private void deleteBlocksInDataNodes(NodeLocation[] dataNodeLocations) throws IOException {
        boolean fileDoesNotExist = dataNodeLocations == null;
        if (fileDoesNotExist) {
            return ;
        }
        NodeLocation firstNodeLocation = dataNodeLocations[0];
        Socket dataNodeSocket = new Socket(firstNodeLocation.address, firstNodeLocation.port);
        sendDeleteRequest(dataNodeSocket, dataNodeLocations);
        dataNodeSocket.close();
    }

    private void sendDeleteRequest(Socket dataNodeSocket, NodeLocation[] dataNodeLocations) throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(dataNodeSocket.getOutputStream());
        outputStream.writeUTF(MessageType.DELETE_BLOCK_REQUEST.name());
        outputStream.writeObject(new DeleteBlockRequest(rdfsFilename, dataNodeLocations));
        outputStream.flush();
    }

    private void cleanUp() throws IOException {
        nameNodeSocket.close();
    }
}
