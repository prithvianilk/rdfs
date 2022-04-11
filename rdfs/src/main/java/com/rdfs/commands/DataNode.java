package com.rdfs.commands;
package com.journaldev.threads;
import picocli.CommandLine.Command;


public class ProcessClientRequest implements Runnable
{
    private Socket clientSocket;
    public ProcessClientRequest(Socket clientSocket)
    {
        this.clientSocket=clientSocket;
    }

    @Override
    public void run()
    {
        ObjectInputStream objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
        WriteBlockRequest blockRequest=(WriteBlockRequest) ObjectInputStream.readObject();
        File f1 = new File("/" + blockRequest.filename);  
        FileOutputStream fout=new FileOutputStream(String.valueOf(blockRequest.blockNumber));//block number
        boolean isCreated = f1.mkdirs();  
        if(isCreated)
        {  
            System.out.println("Folder is created successfully"); 
            fout.write(blockRequest.block);//write it to block number file
        }
        NodeLocation[] nextDataNodeLocations=blockRequest.dataNodeLocations;
        if(nextDataNodeLocations.length==0)
            return ;
        NodeLocation[] nextDataNodeLocations=Arrays.copyOfRange(nextDataNodeLocations,1,nextDataNodeLocations.length -1);
        NodeLocation nextDataNodeToSend=nextDataNodeLocations[0];

        
        Socket socket=new Socket(nextDataNodeToSend.address,nextDataNodeToSend.port);
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        WriteBlockRequest newBlockRequest=new WriteBlockRequest(blockRequest.byte,nextDataNodeLocations,blockRequest.filename,blockRequest.blockNumber);
        out.writeObject(newBlockRequest);
    }
}


@Command(name = "datanode", description = "Start and configure a datanode on the rdfs cluster.")
public class DataNode implements Runnable {

    @Option(names = { "--name-node-address" }, description = "IP Address of the NameNode")
	private String nameNodeAddress = Constants.DEFAULT_NAME_NODE_ADDRESS;

	@Option(names = { "--name-node-port" }, description = "Communication Port of the NameNode")
	private int nameNodePort = Constants.DEFAULT_NAME_NODE_PORT;

    @Option(names = { "--data-node-port" }, description = "Communication Port of the DataNode")
	private int dataNodePort = Constants.DEFAULT_DATA_NODE_PORT;

    public void sendHeartbeat() throws UnknownHostException
    {
        //1. get location of name node
        //2. start an infinite loop and sleep every 3 seconds
        Socket socket=new Socket(nameNodeAddress,nameNodePort);
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        while(true)
        {
            out.writeUTF("Ping");
            Thread.sleep(3000);
        }
    }

    public void sendHandshakeToNameNode()
    {
        //2. set up a socket
        Socket socket=new Socket(nameNodeAddress,nameNodePort);
        //3. ask the namenode to accept the data node and wait for acceptance
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeUTF(MessageType.HANDSHAKE_REQUEST.name());
        //listen for acceptance
        DataInputStream dataInputStream =new DataInputStream(s.getInputStream());  
        String response = (String) dataInputStream.readUTF();  
    }

    //other functionalities:
    //1.create socket server 
    //2.request from client to x data nodes
    //3.send to other data nodes in a new thread.
    public void sendDataToOtherDataNodes(){
        
        ServerSocket socketServer=new ServerSocket(dataNodePort);
        
        while(true)
        {
            Socket clientSocket=socketServer.accept();
            Thread t1=new Thread(new ProcessClientRequest(clientSocket));
            t1.start();
            
        }
    }
    @Override 
	final public void run() {

    }
}