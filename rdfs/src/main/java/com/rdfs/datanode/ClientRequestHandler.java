package com.rdfs.datanode;

import java.net.Socket;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;

import java.util.Arrays;

import com.rdfs.Constants;
import com.rdfs.NodeLocation;
import com.rdfs.message.MessageType;
import com.rdfs.message.WriteBlockRequest;
import com.rdfs.message.ReadBlockRequest;
import com.rdfs.message.DeleteBlockRequest;

public class ClientRequestHandler implements Runnable
{
    private Socket socket;
    private ObjectInputStream inputStream;
    private String dataPath;

    public ClientRequestHandler(Socket socket, String dataPath) 
    {
        try {
            this.socket = socket;
            this.inputStream = new ObjectInputStream(socket.getInputStream());
            this.dataPath = dataPath;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeBlock() throws Exception {
        WriteBlockRequest writeBlockRequest = (WriteBlockRequest) inputStream.readObject();
        NodeLocation[] dataNodeLocations = writeBlockRequest.dataNodeLocations;
        long blockNumber = writeBlockRequest.blockNumber;
        String filename = writeBlockRequest.filename;
        byte[] blockContent = writeBlockRequest.block;
        String fileDirPath = String.format("%s/%s/", dataPath, writeBlockRequest.filename);
        File fileDir = new File(fileDirPath);
        fileDir.mkdirs();
        String blockFilePath = String.format("%s/%s", fileDirPath, String.valueOf(blockNumber));
        FileOutputStream fileOutputStream = new FileOutputStream(blockFilePath);
        fileOutputStream.write(writeBlockRequest.block);
        fileOutputStream.flush();
        boolean isLastDataNode = dataNodeLocations.length == 1;
        if (!isLastDataNode) {
            NodeLocation[] nextDataNodeLocations = Arrays.copyOfRange(writeBlockRequest.dataNodeLocations, 1, writeBlockRequest.dataNodeLocations.length);
            NodeLocation nextDataNodeToSend = nextDataNodeLocations[0];
            Socket nextDataNodesocket = new Socket(nextDataNodeToSend.address, nextDataNodeToSend.port);
            ObjectOutputStream outputStream = new ObjectOutputStream(nextDataNodesocket.getOutputStream());
            writeBlockRequest.dataNodeLocations = nextDataNodeLocations;
            WriteBlockRequest newBlockRequest = writeBlockRequest;
            outputStream.writeUTF(MessageType.WRITE_BLOCK_REQUEST.name());
            outputStream.writeObject(newBlockRequest);
            outputStream.flush();
        }
    }

    private void readBlock() throws Exception {
        ReadBlockRequest readBlockRequest = (ReadBlockRequest) inputStream.readObject();
        String fileDirPath = String.format("%s/%s/", dataPath, readBlockRequest.filename);
        String blockFilePath = String.format("%s/%s", fileDirPath, String.valueOf(readBlockRequest.blockNumber));
        File file = new File(blockFilePath);
        long blockLength = file.length();
        FileInputStream fileInputStream = new FileInputStream(blockFilePath);
		byte block[] = new byte[(int) blockLength];
		fileInputStream.read(block);
        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        outputStream.writeObject(block);
        outputStream.flush();
    }

    private void deleteBlock() throws Exception {
        DeleteBlockRequest deleteBlockRequest = (DeleteBlockRequest) inputStream.readObject();
        String filename = deleteBlockRequest.filename;
        NodeLocation[] dataNodeLocations = deleteBlockRequest.dataNodeLocations;
        String fileDirPath = String.format("%s/%s/", dataPath, filename);
        File file = new File(fileDirPath);
        for (File blockFile : file.listFiles()) {
            blockFile.delete();
        }
        boolean isLastDataNode = dataNodeLocations.length == 1;
        if (!isLastDataNode) {
            NodeLocation[] nextDataNodeLocations = Arrays.copyOfRange(dataNodeLocations, 1, dataNodeLocations.length);
            NodeLocation nextDataNodeToSend = nextDataNodeLocations[0];
            Socket nextDataNodesocket = new Socket(nextDataNodeToSend.address, nextDataNodeToSend.port);
            ObjectOutputStream outputStream = new ObjectOutputStream(nextDataNodesocket.getOutputStream());
            deleteBlockRequest.dataNodeLocations = nextDataNodeLocations;
            DeleteBlockRequest newBlockRequest = deleteBlockRequest;
            outputStream.writeUTF(MessageType.DELETE_BLOCK_REQUEST.name());
            outputStream.writeObject(newBlockRequest);
            outputStream.flush();
        }
    }

    @Override
    public void run()
    {
        try {
            MessageType messageType = MessageType.valueOf(inputStream.readUTF());
            switch (messageType) {
                case WRITE_BLOCK_REQUEST:
                    writeBlock();
                    break;
                case READ_BLOCK_REQUEST:
                    readBlock();
                    break;
                case DELETE_BLOCK_REQUEST:
                    deleteBlock();
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
