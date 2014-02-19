package com.github.bhagatsingh.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.bhagatsingh.websockets.services.WebSocketsService;

/**
 * The GluZooKeeperClient class will create and maintain ZooKeeper Session for a ZNode. 
 * @author Bhagat Singh
 *
 */
public class GluZooKeeperClient implements Watcher{
    private int sessionTimeout;
    private ZooKeeper zookeeper;
    private String host;
    private boolean dead;
    private String applicationsZNode;
    final CountDownLatch connectedSignal = new CountDownLatch(1);
    private static final Logger LOG = LoggerFactory.getLogger(GluZooKeeperClient.class);
    private final Map<String, ZkNodeWatcher> watchers = new HashMap<String, ZkNodeWatcher>();
    private final Map<String, String> zkNodeData = new HashMap<String, String>();
    private boolean isWatcherOn;
    private final WebSocketsService wsService; 
    
    public GluZooKeeperClient(String host, String zNode, int sessionTimeout, boolean watcher, WebSocketsService wsService) throws IOException, KeeperException, InterruptedException{
        this.host = host;
        this.sessionTimeout = sessionTimeout;
        this.applicationsZNode = zNode;
        this.isWatcherOn = watcher;
        this.wsService = wsService; 
    }
    
   /**
    * The connect method is used to connect with ZooKeeper on a znode.
    * @param watcher
    * @return
    * @throws IOException
    * @throws InterruptedException
    */
    public void connect() throws IOException, InterruptedException {
        //Make connection with ZooKeeper
        if(isWatcherOn){
            this.zookeeper =  new ZooKeeper(host, sessionTimeout, this);
            connectedSignal.await();
            try {
                this.setWatcherOnZNodes();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
            this.keepMeAlive();
        }else{
            this.zookeeper = new ZooKeeper(host, sessionTimeout, null);
        }
        System.out.println("#### Connected with Zookeeper Successfully. ####");
    }
    
    /**
     * The disconnect method disconnect connected client from ZooKeeper.
     * @return
     */
    public boolean disconnect() {
        try {
            //close ZooKeeper connection if it is still active
            watchers.clear();
            if (this.zookeeper != null) {
                this.zookeeper.close();
                this.zookeeper = null;
            }
            //Release session Instance
            closing(KeeperException.Code.SESSIONEXPIRED.intValue());
        } catch (Exception e) {
            LOG.error("Error occurred while disconnecting from ZooKeeper server", e);
            return false;
        }
        System.out.println("#### Zookeeper connection got disconnected Successfully. ####");
        return true;
    }
    
    /**
     * 
     * @param rc
     */
    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }
    
    /**
     * to keep the ZooKeeper Client Instance alive till it gets Interrupted OR session gets expired OR session gets closed.
     */
    public void keepMeAlive() {
        try {
            synchronized (this) {
                while (!dead) {
                    System.out.println("#### ZooKeeper Client is connected till it gets Disconnected or Interrupted ####");
                    wait();
                }
            }
        } catch (InterruptedException e) {
            watchers.clear();
            if (this.zookeeper != null) {
                try {
                    this.zookeeper.close();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                this.zookeeper = null;
            }
            e.printStackTrace();
        }
    }
    
    
    /**
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @SuppressWarnings("unused")
    public void setWatcherOnZNodes() throws KeeperException, InterruptedException {
       Stat stat = zookeeper.exists(applicationsZNode, this);
       System.out.println("ZNode "+applicationsZNode +" Version is "+ stat.getVersion());
       //if applications zNode doesn't exists then create it.
       if (stat != null) {
           //get all child nodes of the applications node and set the watcher on it.
           addWatchers(zookeeper.getChildren(applicationsZNode, this));
       }else if(stat == null){
           zookeeper.create(applicationsZNode,null /*data*/,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
       }else{
          //do nothing - not sure when it will come here.... so we are fine. 
       }
       //send applications information to the client
       notifyAppChanges();
    }

    /**
     * process method gets called when there is a change in zNode
     *  @param event
     */
    public void process(WatchedEvent event) {
        String path = event.getPath();
        System.out.println("############## GluZooKeeperSession Change Event fired ###########################");
        System.out.println("path " + path + " state " + event.getState() + " type " + event.getType());
        
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the connection has changed
            switch (event.getState()) {
                case SyncConnected:
                    connectedSignal.countDown();
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    closing(KeeperException.Code.SESSIONEXPIRED.intValue());
                    disconnect();
                    break;
                default:
                    break;
            }
        }else {
            try {
                setWatcherOnZNodes();
            } catch (KeeperException | InterruptedException e ) {
                e.printStackTrace();
            } 
        }
    }
    
    /**
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> list() throws KeeperException, InterruptedException {
       List<String> memberList = new ArrayList<String>();
        List<String> children = zookeeper.getChildren(applicationsZNode, false);
        if (!children.isEmpty()) {
            for (String child : children) {
                memberList.add(child);
            }
        }
        return memberList;
    }
    
    /**
     * 
     * @param znodes
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean remove(List<String> znodes) throws InterruptedException, KeeperException{
        if(znodes!=null && !znodes.isEmpty()){
           for(String znode: znodes){
               zookeeper.delete(applicationsZNode+"/"+znode, -1);
           }
        }
        return true;
    }
   
    /**
     * 
     * @param zNodes
     */
    public void addWatchers(List<String> zNodes) {
        // add watcher for each node and add node to collection of
        // watched nodes
        if(zNodes != null && !zNodes.isEmpty()){
            for (String node : zNodes) {
                //make it absolute node
                node = applicationsZNode+"/"+ node;
                if (!watchers.containsKey(node)) {
                    try {
                        watchers.put(node, new ZkNodeWatcher(node, zookeeper));
                    } catch (Exception e) {
                       System.out.println("Error occured adding node watcher for node: " + node +" : Exception: "+ e.getMessage());
                    }
                }
            }
        }
    }
    
    /**
     * 
     * @param selectedNodes
     */
    public void removeWatchers(List<String> selectedNodes) {
        // remove watcher for each node and remove node from
        // collection of watched nodes
        for (String node : selectedNodes) {
            if (watchers.containsKey(node)) {
                ZkNodeWatcher watcher = watchers.remove(node);
                if (watcher != null) {
                    watcher.stop();
                }
            }
        }
    }
    
    /*
     * The getData method returns zkNode data in String format
     * 
     */
    public String getZkNodeData(String nodePath) {
        try {
            if (nodePath != null && !nodePath.isEmpty()) {
                Stat s = zookeeper.exists(nodePath, false);
                if (s != null) {
                    byte[] encrypted = zookeeper.getData(nodePath, false, s);
                    if(encrypted == null){ 
                        return new String(new byte[0]);
                    }else{
                        return new String(encrypted);
                    }
                }
            }
        } catch (Exception e) {
           System.out.println("Error occurred getting data for node: " + nodePath +" : Exception: "+ e.getMessage());
        }
        return null;
    }
    
    
   /**
    * 
    * @author bsingh
    *
    */
    public class ZkNodeWatcher implements Watcher {

        private final String nodePath;
        private final ZooKeeper zk;
        private boolean closed = false;

        /**
         * 
         * @param nodePath
         * @param zookeeper
         * @throws KeeperException
         * @throws InterruptedException
         */
        public ZkNodeWatcher(String nodePath, ZooKeeper zookeeper) throws KeeperException,InterruptedException {
            this.nodePath = nodePath;
            this.zk = zookeeper;
            Stat stat = zk.exists(nodePath, this);
            setZNodeData(stat);
        }
        
        /**
        * 
        * @param stat
        */
        private void setZNodeData(Stat stat){
            if (stat != null) {
                byte[] encrypted;
                try {
                    encrypted = zookeeper.getData(nodePath, false, stat);
                    if(encrypted != null){ 
                        String zNodeData = new String(encrypted);
                        System.out.println("Data In Zookeeper: "+ zNodeData );
                        zkNodeData.put(nodePath, zNodeData);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                } 
            }
        }
        
        /**
         *  @param event
         * 
         */
        /*
         *  None (-1),
            NodeCreated (1),
            NodeDeleted (2),
            NodeDataChanged (3),
            NodeChildrenChanged (4);
         */
        public void process(WatchedEvent event) {
            System.out.println("############## ZkNodeWatcher Change Event fired ###########################");
            if (!closed) {
                try {
                    if (event.getType() != EventType.NodeDeleted) {
                        Stat s = zk.exists(nodePath, this);
                        //set changed zkNode data
                        setZNodeData(s);
                    }else if (event.getType() == EventType.NodeDeleted){
                        zkNodeData.remove(nodePath);
                    }
                    notifyAppChanges();
                } catch (Exception e) {
                    System.out.println("Error occured re-adding node watcherfor node " + nodePath +" : Exception: "+ e.getMessage());
                }
                
            }
        }

        /**
         * 
         */
        public void stop() {
            this.closed = true;
        }
    }
   
    /**
     * 
     */
    public void notifyAppChanges(){
        System.out.println("Notifying changes to the WS Service......");
        wsService.notifyAppChanges();
    }
    
    /**
     * 
     * @return
     */
    public String getApplications(){
        StringBuilder apps = new StringBuilder("");
        if(zkNodeData !=null && !zkNodeData.isEmpty()){
            apps.append("[");
            Iterator<Entry<String, String>> it = zkNodeData.entrySet().iterator();
            String delim = "";
            while (it.hasNext()) {
                Entry<String, String> pairs = it.next();
                apps.append(delim).append(pairs.getValue());
                delim = ",";
            } 
            apps.append("]");
        }
        System.out.println("Applications From Zookeeper: "+ apps.toString());
        return apps.toString();
    }
}
