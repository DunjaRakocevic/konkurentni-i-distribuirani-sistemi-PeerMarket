package raf.rs.peermarket.config;

import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {

    private ConfigLoader() {}



    public static NodeConfig load(String fileName) {




        try {


            Properties properties = new Properties();

            InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName);

            if (inputStream == null) {
                throw new RuntimeException("Config file not found: " + fileName);
            }


            properties.load(inputStream);



            int nodePort = Integer.parseInt(properties.getProperty("node.port"));


            String bootstrapIp = properties.getProperty("bootstrap.ip");

            int bootstrapPort = Integer.parseInt(properties.getProperty("bootstrap.port"));
            int weakFailMs = Integer.parseInt(properties.getProperty("weak.fail.ms"));
            int strongFailMs = Integer.parseInt(properties.getProperty("strong.fail.ms"));



            return new NodeConfig(
                    nodePort,
                    bootstrapIp,
                    bootstrapPort,
                    weakFailMs,
                    strongFailMs
            );

        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration.", e);
        }
    }
}