package raf.rs.peermarket;

import raf.rs.peermarket.config.ConfigLoader;
import raf.rs.peermarket.config.NodeConfig;
import raf.rs.peermarket.node.PeerNode;

public class App {

    public static void main(String[] args) {

        NodeConfig config = ConfigLoader.load("node.properties");
//        NodeConfig config = ConfigLoader.load("node2.properties");
//        NodeConfig config = ConfigLoader.load("node3.properties");
//        NodeConfig config = ConfigLoader.load("node4.properties");

        PeerNode node = new PeerNode(config);
        node.start();
    }
}