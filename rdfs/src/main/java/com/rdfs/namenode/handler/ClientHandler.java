package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.rdfs.message.MessageType;
import com.rdfs.namenode.DataNodeLocationStore;

public class ClientHandler extends Handler {
    private DataNodeLocationStore dataNodeLocationStore;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;

    public ClientHandler() {
        dataNodeLocationStore = DataNodeLocationStore.getDataNodeLocationStore();
    }

    private void getNewDataNodeLocations() throws IOException {
        var fileName = inputStream.readUTF();
        var dataNodeLocations = dataNodeLocationStore.addBlockNodeLocations(fileName);
        outputStream.writeObject(dataNodeLocations);
    }

    private void getDataNodeLocations() throws IOException {
        var fileName = inputStream.readUTF();
        var dataNodeLocations = dataNodeLocationStore.getDataNodeLocations(fileName);
        outputStream.writeObject(dataNodeLocations);
    }

    private void getBlockLocations() throws IOException {
        var fileName = inputStream.readUTF();
        var blockLocations = dataNodeLocationStore.getBlockLocations(fileName);
        dataNodeLocationStore.deleteFileMetaData(fileName);
        outputStream.writeObject(blockLocations);
    }

    @Override
    public void run() {
        try {
            inputStream = new ObjectInputStream(socket.getInputStream());
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            var messageType = MessageType.valueOf(inputStream.readUTF());
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
