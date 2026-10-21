package raf.rs.peermarket.util;

public class ChordUtil {

    public static boolean inInterval(int id, int start, int end) {

        if (start < end) {
            return id > start && id < end;
        } else {
            return id > start || id < end;
        }
    }
}