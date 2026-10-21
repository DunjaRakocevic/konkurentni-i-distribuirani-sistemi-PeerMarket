package raf.rs.peermarket.config;

public class NodeConfig {

    private final int nodePort;

    private final String bootstrapIp;

    private final int bootstrapPort;

    private final int weakFailMs;

    private final int strongFailMs;

    public NodeConfig(
            int nodePort,
            String bootstrapIp,
            int bootstrapPort,
            int weakFailMs,
            int strongFailMs) {

        this.nodePort = nodePort;
        this.bootstrapIp = bootstrapIp;
        this.bootstrapPort = bootstrapPort;
        this.weakFailMs = weakFailMs;
        this.strongFailMs = strongFailMs;
    }

    public int getNodePort() {
        return nodePort;
    }

    public String getBootstrapIp() {
        return bootstrapIp;
    }

    public int getBootstrapPort() {
        return bootstrapPort;
    }

    public int getWeakFailMs() {
        return weakFailMs;
    }

    public int getStrongFailMs() {
        return strongFailMs;
    }

    @Override
    public String toString() {
        return "NodeConfig{" +
                "nodePort=" + nodePort +
                ", bootstrapIp='" + bootstrapIp + '\'' +
                ", bootstrapPort=" + bootstrapPort +
                ", weakFailMs=" + weakFailMs +
                ", strongFailMs=" + strongFailMs +
                '}';
    }
}