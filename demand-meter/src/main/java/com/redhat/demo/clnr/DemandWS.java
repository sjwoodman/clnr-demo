package com.redhat.demo.clnr;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

/**
 *
 * @author hhiden
 */
@ServerEndpoint("/ws")
@ApplicationScoped
public class DemandWS {
    private static final Logger logger = Logger.getLogger(DemandWS.class.getName());
    
    public static final Map<String, Session> clients = new ConcurrentHashMap<>();
    
    @OnMessage
    public void shout(String text, Session client) {
        logger.info("Ignoring input message: " + text);
    }    
    
    @OnOpen
    public void socketOpened(Session client){
        logger.info("Socked Opened: " + client.getId());
        clients.put(client.getId(), client);
    }
    
    @OnClose
    public void socketClosed(Session client){
        logger.info("Socket Closed: " + client.getId());
        clients.remove(client.getId());
    }
    
    public static void sendDemand(JsonObject demandMessage){
        for(Session client : clients.values()){
            client.getAsyncRemote().sendText(demandMessage.toString());
        }
    }
}
