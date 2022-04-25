package com.rdfs.namenode.handler;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.rdfs.message.MessageType;
import com.rdfs.namenode.DataNodeLocationStore;
import com.rdfs.NodeLocation;
import com.rdfs.BlockReplicasLocation;

public class ClientHandler extends Handler {
    private DataNodeLocationStore dataNodeLocationStore;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;

    public ClientHandler() {
        dataNodeLocationStore = DataNodeLocationStore.getDataNodeLocationStore();
    }

    private void getNewDataNodeLocations() throws IOException {
        String fileName = inputStream.readUTF();
        NodeLocation[] dataNodeLocations = dataNodeLocationStore.addBlockNodeLocations(fileName);
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
        BlockReplicasLocation[] blockLocations = dataNodeLocationStore.getBlockLocations(fileName);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
