package com.github.bhagatsingh.websockets.services;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.zookeeper.KeeperException;

import com.github.bhagatsingh.zookeeper.GluZooKeeperClient;

/**
 * 
 * @author Bhagat Singh
 *
 */
@ServerEndpoint(value = "/applications")
public class WebSocketsService {
	private static final Set< Session > sessions = Collections.synchronizedSet( new HashSet< Session >() );	
	private GluZooKeeperClient zkClient;
	String ZNODE = "/org/glu/agents/applications";
	
	@OnOpen
	public void onOpen( final Session session ) {
		sessions.add( session );
		try {
		    if(zkClient == null){
                zkClient = new GluZooKeeperClient("devpmapp1:2181", ZNODE, 5000, true, this);
                System.out.println("I am going to connect with Zookeeper now..............");
                zkClient.connect();
                System.out.println("I am going to disconnect from ZooKeper now..............");
		    }
		} catch (IOException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
	}

	@OnClose
	public void onClose( final Session session ) {
		sessions.remove( session );
		System.out.println("WebSocket Connection got disconnected...");
		if(zkClient != null){
		    zkClient.disconnect();
		    System.out.println("Asked ZooKeeper Client to Disconnect from ZooKeeper.....");
		    zkClient = null;
		}
	}
	
	/**
	 * 
	 * @param message
	 * @param client
	 * @throws IOException
	 * @throws EncodeException
	 * @throws InterruptedException
	 */
	@OnMessage
	public void onMessage( final String message, final Session client ) throws IOException, EncodeException, InterruptedException {
		/*for( final Session session: sessions) {
		    session.getBasicRemote().sendText( getApps() );
		}*/
	    notifyAppChanges();
	}
	
	/**
	 *The notifyAppChanges method notifies connected client(s) about application state change
	 * 
	 */
	public void notifyAppChanges(){
	    System.out.println("I got notified about App Changes.. Notifying Client about Changes........");
	    for( final Session session: sessions) {
            try {
                session.getBasicRemote().sendText( zkClient.getApplications() );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}
}
