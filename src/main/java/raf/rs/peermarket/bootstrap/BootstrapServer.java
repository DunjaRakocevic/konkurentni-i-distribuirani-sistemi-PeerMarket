package raf.rs.peermarket.bootstrap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BootstrapServer {



    private final int port;
    private final Set<String> activeNodes = ConcurrentHashMap.newKeySet();



    public BootstrapServer(int port) {
        this.port = port;
    }

    public void start() {

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println(
                    "Bootstrap server started on port " + port);

            while (true) {

                Socket socket = serverSocket.accept();

                new Thread(() -> handleClient(socket)).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



//    private void handleClient(Socket socket) {
//
//        try (
//                BufferedReader reader =
//                        new BufferedReader(
//                                new InputStreamReader(
//                                        socket.getInputStream()));
//
//                PrintWriter writer =
//                        new PrintWriter(
//                                socket.getOutputStream(), true)
//        ) {
//
//            String command = reader.readLine();
//
//            if (!"HAIL".equals(command)) {
//                return;
//            }
//
//            String newNodeAddress = reader.readLine();
//
//            if (activeNodes.isEmpty()) {
//
//                activeNodes.add(newNodeAddress);
//
//                writer.println("FIRST");
//
//            } else {
//
//                String contactNode =
//                        activeNodes.iterator().next();
//
//                activeNodes.add(newNodeAddress);
//
//                writer.println("CONTACT");
//                writer.println(contactNode);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }



    private void handleClient(Socket socket) {

        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {

            String cmd = in.readLine();

            if (!"HAIL".equals(cmd)) return;

            String node = in.readLine();
            activeNodes.add(node);

            if (activeNodes.size() == 1) {
                out.println("FIRST");
            } else {
                out.println("CONTACT");
                out.println(activeNodes.iterator().next());
            }

            out.println(String.join(";", activeNodes));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        BootstrapServer bootstrapServer =
                new BootstrapServer(2000);

        bootstrapServer.start();
    }
}
