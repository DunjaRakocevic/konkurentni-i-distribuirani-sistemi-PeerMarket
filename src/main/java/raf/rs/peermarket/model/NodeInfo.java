package raf.rs.peermarket.model;

import raf.rs.peermarket.util.HashUtil;

import java.io.Serializable;

public class NodeInfo implements Serializable {

    private final String ip;
    private final int port;
    private final int nodeId;

    public NodeInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
        this.nodeId = HashUtil.hash(ip + ":" + port);
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getNodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return nodeId + "(" + ip + ":" + port + ")";
    }
}