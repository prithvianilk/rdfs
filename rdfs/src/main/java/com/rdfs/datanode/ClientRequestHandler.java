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
        WriteBlockRequest blockRequest = (WriteBlockRequest) inputStream.readObject();
        String fileDirPath = String.format("%s/%s/", dataPath, blockRequest.filename);
        File fileDir = new File(fileDirPath);
        fileDir.mkdirs();
        String blockFilePath = String.format("%s/%s", fileDirPath, String.valueOf(blockRequest.blockNumber));
        FileOutputStream fileOutputStream = new FileOutputStream(blockFilePath);
        fileOutputStream.write(blockRequest.block);
        fileOutputStream.flush();
        NodeLocation[] nextDataNodeLocations = Arrays.copyOfRange(blockRequest.dataNodeLocations, 1, blockRequest.dataNodeLocations.length);
        if (nextDataNodeLocations.length != 0) {
            NodeLocation nextDataNodeToSend = nextDataNodeLocations[0];
            Socket nextDataNodesocket = new Socket(nextDataNodeToSend.address, nextDataNodeToSend.port);
            ObjectOutputStream outputStream = new ObjectOutputStream(nextDataNodesocket.getOutputStream());
            WriteBlockRequest newBlockRequest = new WriteBlockRequest(blockRequest.block, nextDataNodeLocations, blockRequest.filename, blockRequest.blockNumber);
            outputStream.writeUTF(MessageType.WRITE_BLOCK_REQUEST.name());
            outputStream.writeObject(newBlockRequest);
            outputStream.flush();
        }
    }

    private void readBlock() throws Exception {
        ReadBlockRequest blockRequest = (ReadBlockRequest) inputStream.readObject();
        String fileDirPath = String.format("%s/%s/", dataPath, blockRequest.filename);
        String blockFilePath = String.format("%s/%s", fileDirPath, String.valueOf(blockRequest.blockNumber));
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
        String filename = inputStream.readUTF();
        String fileDirPath = String.format("%s/%s/", dataPath, filename);
        File file = new File(fileDirPath);
        for (File blockFile : file.listFiles()) {
            blockFile.delete();
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
