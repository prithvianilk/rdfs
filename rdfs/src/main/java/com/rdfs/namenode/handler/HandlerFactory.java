package com.rdfs.namenode.handler;

public class HandlerFactory {
    public static Handler createHandler(HandlerType handlerType) throws Exception {
        if (handlerType == null) {
            throw new Exception("Handler type is invalid: " + handlerType);
        }

        switch (handlerType) {
            case HEARTBEAT:
                return new HeartbeatHandler();

            case CLIENT:
                return new ClientHandler();
        
            default:
                throw new Exception("Handler type is invalid: " + handlerType);
        }
    }
}
