package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.rdfs.message.MessageType;
import com.rdfs.message.GetNewDataNodeLocationsRequest;
import com.rdfs.namenode.DataNodeLocationStore;
import com.rdfs.NodeLocation;

public class ClientHandler extends Handler {
    private DataNodeLocationStore dataNodeLocationStore;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;

    public ClientHandler() {
        dataNodeLocationStore = DataNodeLocationStore.getDataNodeLocationStore();
    }

    private void getNewDataNodeLocations() throws IOException, ClassNotFoundException {
        GetNewDataNodeLocationsRequest getNewDataNodeLocationsRequest = (GetNewDataNodeLocationsRequest) inputStream.readObject();
        String filename = getNewDataNodeLocationsRequest.filename;
        boolean isNewWrite = getNewDataNodeLocationsRequest.isNewWrite;
        if (isNewWrite) {
            dataNodeLocationStore.deleteFileMetaData(filename);
        }
        NodeLocation[] dataNodeLocations = dataNodeLocationStore.addBlockNodeLocations(filename);
        outputStream.writeObject(dataNodeLocations);
        outputStream.flush();
    }

    private void getDataNodeLocations() throws IOException {
        String fileName = inputStream.readUTF();
        NodeLocation[] dataNodeLocations = dataNodeLocationStore.getDataNodeLocations(fileName);
        outputStream.writeObject(dataNodeLocations);
        outputStream.flush();
    }

    private void getBlockLocations() throws IOException {
        String fileName = inputStream.readUTF();
        NodeLocation[] blockLocations = dataNodeLocationStore.getBlockLocations(fileName);
        dataNodeLocationStore.deleteFileMetaData(fileName);
        outputStream.writeObject(blockLocations);
        outputStream.flush();
    }

    @Override
    public void run() {
        try {
            inputStream = new ObjectInputStream(socket.getInputStream());
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            MessageType messageType = MessageType.valueOf(inputStream.readUTF());
            switch (messageType) {
                case GET_NEW_DATANODE_LOCATIONS_REQUEST:
                    getNewDataNodeLocations();
                    break;
                case GET_DATANODE_LOCATIONS_REQUEST:
                    getDataNodeLocations();
                    break;
                case GET_BLOCK_LOCATIONS_REQUEST:
                    getBlockLocations();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
