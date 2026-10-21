package raf.rs.peermarket.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class HashUtil {

    public static int hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            int hash =
                    ((digest[0] & 0xFF) << 24) |
                            ((digest[1] & 0xFF) << 16) |
                            ((digest[2] & 0xFF) << 8) |
                            (digest[3] & 0xFF);

            return Math.abs(hash);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}