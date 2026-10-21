package raf.rs.peermarket.node;

import raf.rs.peermarket.config.NodeConfig;
import raf.rs.peermarket.model.Item;
import raf.rs.peermarket.model.NodeInfo;
import raf.rs.peermarket.storage.MarketStorage;
import raf.rs.peermarket.util.ChordUtil;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class PeerNode {

    private final NodeConfig config;

    private NodeInfo self;
    private NodeInfo successor;
    private NodeInfo predecessor;

    private final MarketStorage storage = new MarketStorage();
    private final Set<NodeInfo> subscribers = ConcurrentHashMap.newKeySet();
    private final Set<Integer> nodesInSystem = ConcurrentHashMap.newKeySet();
    private final Map<Integer, NodeInfo> knownNodes = new ConcurrentHashMap<>();
    private final Set<Integer> takeoverInProgress = ConcurrentHashMap.newKeySet();
//    private final Set<Integer> repliesPending = ConcurrentHashMap.newKeySet();
//    private final Set<Integer> deferredReplies = ConcurrentHashMap.newKeySet();
    private final Set<String> visitedSearch = ConcurrentHashMap.newKeySet();
    private final Set<Integer> rejoiningNodes = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Long> lastSeen = new ConcurrentHashMap<>();
//    private final Map<Integer, Integer> requestTimestamps = new ConcurrentHashMap<>();
    private final Set<String> searchResultsPrinted = ConcurrentHashMap.newKeySet();



    private int lamportClock = 0;
//    private int expectedReplies = 0;
//    private boolean inCriticalSection = false;
//    private boolean requestingCS = false;



    private enum State {
        RELEASED,
        WANTED,
        HELD
    }
    private State state = State.RELEASED;



    private int requestTimestamp = -1;

    private final Queue<Integer> deferredQueue = new ConcurrentLinkedQueue<>();
    private final Set<Integer> repliesReceived = ConcurrentHashMap.newKeySet();


    private int buyItemId;
    private int buyQty;

    public PeerNode(NodeConfig config) {
        this.config = config;
    }

    public void start() {

        try {
            self = new NodeInfo("127.0.0.1", config.getNodePort());

            knownNodes.put(
                    self.getNodeId(),
                    self
            );

            nodesInSystem.add(
                    self.getNodeId()
            );

            startServer();

            NodeInfo contact = contactBootstrap();

            joinNetwork(contact);

            startHeartbeatSender();

            startFailureDetector();

            startStabilizer();

            startCli();

            startRejoinCleaner();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void startCli() {

        new Thread(() -> {

            Scanner sc = new Scanner(System.in);

            while (true) {

                String line = sc.nextLine();

                String[] parts = line.split(" ");

                switch (parts[0]) {

                    case "list_item":
                        listItem(
                                Integer.parseInt(parts[1]),
                                parts[2],
                                Integer.parseInt(parts[3])
                        );
                        break;

                    case "search":
                        search(parts[1]);
                        break;

                    case "buy":
                        buy(
                                Integer.parseInt(parts[1]),
                                Integer.parseInt(parts[2])
                        );
                        break;

                    case "stop":
                        System.exit(0);
                        break;

                    case "nodes":
                        System.out.println(nodesInSystem);
                        break;
                }
            }

        }).start();
    }

    private final Set<Integer> weakSuspects = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Integer> weakConfirmCounter = new ConcurrentHashMap<>();

    private void startFailureDetector() {

        ScheduledExecutorService exec =
                Executors.newSingleThreadScheduledExecutor();

        exec.scheduleAtFixedRate(() -> {

            long now = System.currentTimeMillis();

            for (Integer nodeId : new HashSet<>(nodesInSystem)) {

                if (nodeId == self.getNodeId()) continue;

                long last = lastSeen.getOrDefault(nodeId, 0L);
                long diff = now - last;



                if (diff > config.getWeakFailMs() && diff <= config.getStrongFailMs()) {

                    if (weakSuspects.add(nodeId)) {
                        weakConfirmCounter.put(nodeId, 1);
                    } else {
                        int c = weakConfirmCounter.getOrDefault(nodeId, 0) + 1;
                        weakConfirmCounter.put(nodeId, c);

                        if (c % 3 == 0) {
                            System.out.println("[SYS-WEAK-FAIL] node:" + nodeId);
                        }
                    }
                }else {
                    weakSuspects.remove(nodeId);
                    weakConfirmCounter.remove(nodeId);
                }



                if (diff > config.getStrongFailMs()) {

                    System.out.println("[SYS-STRONG-FAIL] node:" + nodeId);

                    handleNodeFailure(nodeId);

                    weakSuspects.remove(nodeId);
                    weakConfirmCounter.remove(nodeId);
                    lastSeen.remove(nodeId);
                }
            }

        }, 2, 2, TimeUnit.SECONDS);
    }


    private void sendSearchResult(int destinationId, Item item) {

        NodeInfo node = resolveNode(destinationId);

        if (node == null) return;

        try (
                Socket socket =
                        new Socket(node.getIp(), node.getPort());

                PrintWriter out =
                        new PrintWriter(socket.getOutputStream(), true)
        ) {

            out.println("SEARCH_RESULT");

            out.println(item.getItemId());
            out.println(item.getName());
            out.println(item.getQuantity());
            out.println(item.getOwnerId());

        } catch (Exception ignored) {}
    }


    private void handleNodeFailure(Integer nodeId) {

        if (!nodesInSystem.contains(nodeId)) return;
        if (!takeoverInProgress.add(nodeId)) {
            return;
        }

        nodesInSystem.remove(nodeId);
        knownNodes.remove(nodeId);
        lastSeen.remove(nodeId);



        rejoiningNodes.remove(nodeId);
        repliesReceived.remove(nodeId);
        weakSuspects.remove(nodeId);
        weakConfirmCounter.remove(nodeId);

        List<Integer> taken = new ArrayList<>();
        List<Item> snapshot = new ArrayList<>(storage.getAll().values());

        for (Item item : snapshot) {

            if (item.getOwnerId() == nodeId) {





                synchronized (storage) {

                    if (item.getOwnerId() == nodeId) {

                        storage.transferOwnership(
                                item.getItemId(),
                                self.getNodeId()
                        );

                        taken.add(item.getItemId());
                    }
                }
            }
        }

        System.out.println("[SYS-BACKUP-TAKEOVER] node:"
                + nodeId + " item_ids:" + takenToString(taken));

        recomputeNeighbors();

        nodesInSystem.addAll(knownNodes.keySet());
    }



    private void recomputeNeighbors() {

        List<NodeInfo> sorted = new ArrayList<>();

        for (Integer id : nodesInSystem) {
            NodeInfo node = knownNodes.get(id);
            if (node != null) sorted.add(node);
        }

        if (sorted.size() <= 1) {
            successor = self;
            predecessor = self;
            printNeighbors();
            return;
        }

        sorted.sort(Comparator.comparingInt(NodeInfo::getNodeId));

        int myId = self.getNodeId();
        int idx = -1;

        for (int i = 0; i < sorted.size(); i++) {
            if (sorted.get(i).getNodeId() == myId) {
                idx = i;
                break;
            }
        }



        // fallback
        if (idx == -1) {
            successor = sorted.get(0);
            predecessor = sorted.get(sorted.size() - 1);
            printNeighbors();
            return;
        }


        predecessor = sorted.get((idx - 1 + sorted.size()) % sorted.size());
        successor = sorted.get((idx + 1) % sorted.size());

        printNeighbors();
    }



    private String takenToString(List<Integer> list) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < list.size(); i++) {

            sb.append(list.get(i));

            if (i < list.size() - 1) sb.append(",");
        }

        return sb.toString();
    }

    private void startServer() {

        new Thread(() -> {

            try (ServerSocket ss = new ServerSocket(config.getNodePort())) {

                while (true) {
                    handleRequest(ss.accept());
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();
    }


    private void handleRequest(Socket socket) {

        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {

            String cmd = in.readLine();

            if ("PING".equals(cmd)) {

                int nodeId = Integer.parseInt(in.readLine());
                String addr = in.readLine();

                String[] p = addr.split(":");
                NodeInfo node = new NodeInfo(p[0], Integer.parseInt(p[1]));

                boolean newNode = !knownNodes.containsKey(nodeId);

                knownNodes.put(nodeId, node);
                nodesInSystem.add(nodeId);
                lastSeen.put(nodeId, System.currentTimeMillis());



                if (newNode) {
                    System.out.println("[NODE-DISCOVERY] new node via PING: " + nodeId);
                }

                if (!nodesInSystem.contains(nodeId)) {
                    System.out.println("[NODE-REJOIN-DETECTED] node:" + nodeId);
                    rejoiningNodes.add(nodeId);
                }

                NodeInfo existing = knownNodes.get(nodeId);
                if (existing == null) {
                    System.out.println("[DISCOVERY] new node seen via PING: " + nodeId);
                }

                out.println("PONG");
            }

            if ("PONG".equals(cmd)) {

                int nodeId = Integer.parseInt(in.readLine());
                lastSeen.put(nodeId, System.currentTimeMillis());
            }

            if ("LIST_ITEM".equals(cmd)) {

                String[] data = in.readLine().split(",");

                int itemId = Integer.parseInt(data[0]);
                String name = data[1];
                int qty = Integer.parseInt(data[2]);
                int originId = Integer.parseInt(data[3]);

                NodeInfo owner = findSuccessor(itemId);

                if (owner.getNodeId() == self.getNodeId()) {

                    storage.addItem(new Item(itemId, name, qty, self.getNodeId()));

                    System.out.println("[MARKET-LIST] item_id:" + itemId + " name:\"" + name + "\" qty:" + qty);

                    replicateToSuccessor(itemId, name, qty);
                    notifySubscribers(itemId);

                } else if (self.getNodeId() != originId) {

                    forwardListItem(successor, itemId, name, qty, originId);
                }
            }

            if ("REPLICA_ITEM".equals(cmd)) {

                String[] data = in.readLine().split(",");

                storage.addItem(new Item(
                        Integer.parseInt(data[0]),
                        data[1],
                        Integer.parseInt(data[2]),
                        Integer.parseInt(data[3])
                ));
            }

            if ("NOTIFY".equals(cmd)) {

                int ownerId = Integer.parseInt(in.readLine());
                int itemId = Integer.parseInt(in.readLine());

                System.out.println("[MARKET-NOTIFICATION] node:" + ownerId + " posted item_id:" + itemId);
            }

            if ("SEARCH".equals(cmd)) {

                String name = in.readLine();
                int originId = Integer.parseInt(in.readLine());
                int currentId = Integer.parseInt(in.readLine());

                System.out.println("[SEARCH-RECEIVED] " + name +
                        " from=" + originId +
                        " current=" + currentId);

                searchForward(name, originId, currentId);
            }

            if ("MUTEX_REQUEST".equals(cmd)) {

                int ts = Integer.parseInt(in.readLine());
                int senderId = Integer.parseInt(in.readLine());

                lamportClock = Math.max(lamportClock, ts) + 1;

                boolean hasPriority =
                        state == State.HELD ||
                                (state == State.WANTED &&
                                        (requestTimestamp < ts ||
                                                (requestTimestamp == ts && self.getNodeId() < senderId)));

                if (hasPriority) {
                    deferredQueue.add(senderId);
                } else {
                    sendReply(senderId);
                }
            }


            if ("MUTEX_REPLY".equals(cmd)) {

                int senderId = Integer.parseInt(in.readLine());

                repliesReceived.add(senderId);
                tryEnterCS();
            }

            if ("GET_PREDECESSOR".equals(cmd)) {

                out.println(predecessor != null
                        ? predecessor.getNodeId()
                        : self.getNodeId());
            }

            if ("NOTIFY_PREDECESSOR".equals(cmd)) {

                int nodeId = Integer.parseInt(in.readLine());

                NodeInfo possiblePred = knownNodes.get(nodeId);

                if (possiblePred != null) {

                    if (predecessor == null ||
                            isBetween(nodeId, predecessor.getNodeId(), self.getNodeId())) {

                        predecessor = possiblePred;
                    }
                }
            }

            if ("SEARCH_RESULT".equals(cmd)) {

                int itemId = Integer.parseInt(in.readLine());
                String itemName = in.readLine();
                int qty = Integer.parseInt(in.readLine());
                int ownerId = Integer.parseInt(in.readLine());

                String key = itemId + ":" + ownerId;

                if (!searchResultsPrinted.add(key)) {
                    return;
                }



                System.out.println(
                        "[MARKET-SEARCH-RESULT] item_id:" + itemId +
                                " name:\"" + itemName + "\"" +
                                " qty:" + qty +
                                " owner_id:" + ownerId
                );
            }


            if ("BUY_REQUEST".equals(cmd)) {

                int itemId = Integer.parseInt(in.readLine());
                int qty = Integer.parseInt(in.readLine());

                buy(itemId, qty);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private void handleRejoin(int nodeId) {



        if (!rejoiningNodes.contains(nodeId)) return;

        rejoiningNodes.remove(nodeId);



        System.out.println("[REJOIN-HANDLED] node:" + nodeId);

        recomputeNeighbors();
    }



    private void tryEnterCS() {

        if (state != State.WANTED) return;

        int expected = nodesInSystem.size() - 1;

        if (repliesReceived.size() >= expected) {
            enterCriticalSection();
        }
    }

    private void listItem(int itemId, String name, int qty) {

        NodeInfo owner = findSuccessor(itemId);

        if (owner.getNodeId() == self.getNodeId()) {

            storage.addItem(new Item(itemId, name, qty, self.getNodeId()));

            System.out.println("[MARKET-LIST] item_id:" + itemId + " name:\"" + name + "\" qty:" + qty);

            replicateToSuccessor(itemId, name, qty);

            notifySubscribers(itemId);

        } else {
            forwardListItem(successor, itemId, name, qty, self.getNodeId());
        }
    }

    private boolean isResponsible(int key) {
        return findSuccessor(key).getNodeId() == self.getNodeId();
    }


    private void replicateToSuccessor(int id, String name, int qty) {

        sendReplica(successor, id, name, qty);
        if (predecessor != null) {
            sendReplica(predecessor, id, name, qty);
        }
    }

    private void sendReplica(NodeInfo node, int id, String name, int qty) {

        try (
                Socket socket = new Socket(node.getIp(), node.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("REPLICA_ITEM");
            out.println(id + "," + name + "," + qty + "," + self.getNodeId());
        } catch (Exception ignored) {}
    }




    private void forwardListItem(NodeInfo node, int id, String name, int qty, int originId) {

        if (node == null) return;
        if (node.getNodeId() == self.getNodeId()) return;

        try (
                Socket socket = new Socket(node.getIp(), node.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {
            out.println("LIST_ITEM");
            out.println(id + "," + name + "," + qty + "," + originId);

        } catch (Exception e) {
            System.out.println("[LIST-FORWARD-FAIL] node=" + node.getNodeId());
        }
    }



    public void subscribe(String ip, int port) {
        subscribers.add(new NodeInfo(ip, port));
    }

    private void notifySubscribers(int itemId) {

        for (NodeInfo sub : subscribers) {

            try (
                    Socket socket = new Socket(sub.getIp(), sub.getPort());
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                out.println("NOTIFY");
                out.println(self.getNodeId());
                out.println(itemId);
            } catch (Exception ignored) {}
        }
    }

    public void search(String name) {

        searchResultsPrinted.clear();
        visitedSearch.clear();

        searchForward(name, self.getNodeId(), self.getNodeId());
    }


    private void searchForward(String name, int originId, int currentId) {

        String key = currentId + ":" + name;

        if (visitedSearch.contains(key)) return;
        visitedSearch.add(key);



        for (Item item : storage.getAll().values()) {

            if (item.getName().equalsIgnoreCase(name)) {
                if (originId == self.getNodeId()) {

                System.out.println("[MARKET-SEARCH-RESULT] item_id:"
                        + item.getItemId()
                        + " name:\""
                        + item.getName()
                        + "\" qty:"
                        + item.getQuantity()
                        + " owner_id:"
                        + item.getOwnerId());

                } else {
                    sendSearchResult(originId, item);
                }
            }
        }


        List<Integer> neighbors = new ArrayList<>();

        if (predecessor != null) neighbors.add(predecessor.getNodeId());
        if (successor != null) neighbors.add(successor.getNodeId());




        for (Integer id : nodesInSystem) {
            if (!neighbors.contains(id)) {
                neighbors.add(id);
            }
        }

        for (Integer nodeId : neighbors) {

            if (nodeId == self.getNodeId()) continue;
            if (nodeId == originId) continue;

            NodeInfo next = resolveNode(nodeId);
            if (next == null) continue;

            sendSearch(next, name, originId);
        }
    }

    private void sendSearch(NodeInfo next, String name, int originId) {

        try (Socket socket = new Socket(next.getIp(), next.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("SEARCH");
            out.println(name);
            out.println(originId);
            out.println(self.getNodeId());

        } catch (IOException e) {
            System.out.println("[SEARCH-FAIL] node=" + next.getNodeId());
        }
    }


    public void buy(int itemId, int qty) {

        NodeInfo owner = findSuccessor(itemId);
        if (owner.getNodeId() != self.getNodeId()) {
            sendBuyRequest(owner, itemId, qty);
            return;
        }

        lamportClock++;
        requestTimestamp = lamportClock;

        state = State.WANTED;

        buyItemId = itemId;
        buyQty = qty;

        repliesReceived.clear();

        System.out.println("[MUTEX-REQUEST] item_id:" + itemId);

        broadcastRequest();

        tryEnterCS();
    }



    private void sendBuyRequest(NodeInfo owner,
                                int itemId,
                                int qty) {

        try (
                Socket socket =
                        new Socket(owner.getIp(),
                                owner.getPort());

                PrintWriter out =
                        new PrintWriter(socket.getOutputStream(),
                                true)
        ) {

            out.println("BUY_REQUEST");
            out.println(itemId);
            out.println(qty);

        } catch (Exception ignored) {}
    }



//    public void buy(int itemId, int qty) {
//
//        lamportClock++;
//
//        requestingCS = true;
//
//        buyItemId = itemId;
//        buyQty = qty;
//
//        repliesPending.clear();
//        deferredReplies.clear();
//
//        expectedReplies = nodesInSystem.size() - 1;
//
//        requestTimestamps.put(
//                self.getNodeId(),
//                lamportClock
//        );
//
//        System.out.println(
//                "[MUTEX-REQUEST] item_id:" + itemId);
//
//        broadcastRequest(lamportClock);
//
//        if (expectedReplies == 0) {
//            enterCriticalSection();
//        }
//    }



    private void broadcastRequest() {

        List<Integer> orderedNodes = new ArrayList<>(nodesInSystem);

        List<Integer> prioritized = new ArrayList<>();

        if (predecessor != null) prioritized.add(predecessor.getNodeId());
        if (successor != null) prioritized.add(successor.getNodeId());


        for (Integer id : orderedNodes) {
            if (!prioritized.contains(id)) {
                prioritized.add(id);
            }
        }

        for (Integer nodeId : prioritized) {

            if (nodeId == self.getNodeId()) continue;
            if (!isAliveNode(nodeId)) continue;
            NodeInfo node = resolveNode(nodeId);
            if (node == null) continue;

            try (Socket socket = new Socket(node.getIp(), node.getPort());
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                out.println("MUTEX_REQUEST");
                out.println(requestTimestamp);
                out.println(self.getNodeId());

            } catch (Exception ignored) {}
        }

    }

    private boolean isAliveNode(Integer nodeId) {
        return nodesInSystem.contains(nodeId) &&
                knownNodes.containsKey(nodeId);
    }



    private NodeInfo resolveNode(int id) {

        NodeInfo node = knownNodes.get(id);

        if (node != null) {
            return node;
        }



        System.out.println("[WARN] resolveNode miss for id=" + id);

        return null;
    }



    private void enterCriticalSection() {

        state = State.HELD;

        System.out.println("[MUTEX-ACQUIRED]");

        Item item = storage.getItem(buyItemId);

        if (item == null || item.getQuantity() < buyQty) {
            System.out.println("[MARKET-BUY-FAIL] item_id:" + buyItemId + " reason:OUT_OF_STOCK");
        } else {
            item.decrease(buyQty);
            System.out.println("[MARKET-BUY-SUCCESS] item_id:" + buyItemId
                    + " qty_bought:" + buyQty
                    + " remaining_qty:" + item.getQuantity());
        }

        exitCriticalSection();
    }


    private void exitCriticalSection() {

        state = State.RELEASED;
        requestTimestamp = -1;

        System.out.println("[MUTEX-RELEASED]");

        while (!deferredQueue.isEmpty()) {
            sendReply(deferredQueue.poll());
        }

        repliesReceived.clear();
    }




    private void sendReply(int nodeId) {

        NodeInfo node = resolveNode(nodeId);
        if (node == null) return;

        try (Socket socket = new Socket(node.getIp(), node.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("MUTEX_REPLY");
            out.println(self.getNodeId());

        } catch (Exception ignored) {}
    }

    private void printNeighbors() {

        System.out.println("[SYS-NEIGHBORS] my_id:" + self.getNodeId()
                + " neighbors:" + predecessor.getNodeId() + "," + successor.getNodeId());
    }


    private int hash(int key) {
        return Math.floorMod(key, Integer.MAX_VALUE);
    }

//    private NodeInfo findSuccessor(int keyId) {
//        if (nodesInSystem.isEmpty()) return self;
//
//        List<NodeInfo> sorted = new ArrayList<>();
//        for (Integer id : nodesInSystem) {
//            NodeInfo n = knownNodes.get(id);
//            if (n != null) sorted.add(n);
//        }
//
//        sorted.sort(Comparator.comparingInt(NodeInfo::getNodeId));
//
//        for (NodeInfo n : sorted) {
//            if (ChordUtil.inInterval(keyId,
//                    predecessor.getNodeId(),
//                    n.getNodeId())) {
//                return n;
//            }
//        }
//
//        return successor;
//    }



    private NodeInfo findSuccessor(int keyId) {

        List<NodeInfo> sorted = new ArrayList<>();

        for (Integer id : nodesInSystem) {
            NodeInfo n = knownNodes.get(id);
            if (n != null) sorted.add(n);
        }

        if (sorted.isEmpty()) return self;

        sorted.sort(Comparator.comparingInt(NodeInfo::getNodeId));


        if (keyId <= sorted.get(0).getNodeId()) {
            return sorted.get(0);
        }



        for (int i = 0; i < sorted.size() - 1; i++) {

            int current = sorted.get(i).getNodeId();
            int next = sorted.get(i + 1).getNodeId();

            if (keyId > current && keyId <= next) {
                return sorted.get(i + 1);
            }
        }

        return sorted.get(0);
    }


    private NodeInfo contactBootstrap() {

        try (
                Socket socket = new Socket(config.getBootstrapIp(), config.getBootstrapPort());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
        ) {

            out.println("HAIL");
            out.println(self.getIp() + ":" + self.getPort());

            String response = in.readLine();
            String contactRaw = in.readLine();
            String allNodes = in.readLine();

            loadSystemNodes(allNodes);

            if ("FIRST".equals(response)) return null;

            String[] parts = contactRaw.split(":");

            return new NodeInfo(parts[0], Integer.parseInt(parts[1]));

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void joinNetwork(NodeInfo contact) {

        if (contact != null) {
            knownNodes.put(contact.getNodeId(), contact);
            nodesInSystem.add(contact.getNodeId());
        }

        if (nodesInSystem.size() <= 1) {
            successor = self;
            predecessor = self;
            printNeighbors();
            return;
        }

        List<NodeInfo> sorted = new ArrayList<>();

        for (Integer id : nodesInSystem) {
            NodeInfo node = knownNodes.get(id);
            if (node != null) {
                sorted.add(node);
            }
        }

        sorted.sort(Comparator.comparingInt(NodeInfo::getNodeId));

        NodeInfo pred = self;
        NodeInfo succ = self;

        for (int i = 0; i < sorted.size(); i++) {

            NodeInfo node = sorted.get(i);

            if (node.getNodeId() == self.getNodeId()) {

                pred = sorted.get((i - 1 + sorted.size()) % sorted.size());
                succ = sorted.get((i + 1) % sorted.size());

                break;
            }
        }

        predecessor = pred;
        successor = succ;

        printNeighbors();
    }


    private void loadSystemNodes(String raw) {

        if (raw == null || raw.isEmpty()) {
            return;
        }

        for (String s : raw.split(";")) {

            String[] p = s.split(":");

            NodeInfo node =
                    new NodeInfo(
                            p[0],
                            Integer.parseInt(p[1]));

            nodesInSystem.add(node.getNodeId());

            knownNodes.put(
                    node.getNodeId(),
                    node
            );
        }

        System.out.println("KNOWN NODES");

        for (Map.Entry<Integer, NodeInfo> entry : knownNodes.entrySet()) {

            System.out.println(
                    "id=" + entry.getKey()
                            + " -> "
                            + entry.getValue().getIp()
                            + ":"
                            + entry.getValue().getPort()
            );
        }

        System.out.println("--------\n\n");

    }




    private void startHeartbeatSender() {

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();




        exec.scheduleAtFixedRate(() -> {

            long now = System.currentTimeMillis();

            for (Integer nodeId : nodesInSystem) {

                if (nodeId == self.getNodeId()) continue;

                NodeInfo node = resolveNode(nodeId);
                if (node == null) continue;

                try (Socket socket = new Socket(node.getIp(), node.getPort());
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                    out.println("PING");
                    out.println(self.getNodeId());
                    out.println(self.getIp() + ":" + self.getPort());

                    lastSeen.put(nodeId, now);

                } catch (Exception ignored) {

                }
            }

        }, 1, 1, TimeUnit.SECONDS);
    }




    private void startRejoinCleaner() {

        ScheduledExecutorService exec =
                Executors.newSingleThreadScheduledExecutor();

        exec.scheduleAtFixedRate(() -> {

            for (Integer nodeId : new HashSet<>(rejoiningNodes)) {

                Long last = lastSeen.get(nodeId);

                if (last == null) {
                    rejoiningNodes.remove(nodeId);
                    continue;
                }

                long diff = System.currentTimeMillis() - last;


                if (diff < config.getWeakFailMs()) {
                    handleRejoin(nodeId);
                }
            }

        }, 3, 3, TimeUnit.SECONDS);
    }

    private void startStabilizer() {

        ScheduledExecutorService exec =
                Executors.newSingleThreadScheduledExecutor();

        exec.scheduleAtFixedRate(() -> {

            try {
                stabilize();
            } catch (Exception ignored) {}

        }, 2, 2, TimeUnit.SECONDS);
    }

    private void stabilize() {

        if (successor == null || successor == self) return;

        try (Socket socket = new Socket(successor.getIp(), successor.getPort());
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("GET_PREDECESSOR");

            String response = in.readLine();

            if (response == null) return;

            int predId = Integer.parseInt(response);

            NodeInfo predOfSucc = knownNodes.get(predId);

            if (predOfSucc != null &&
                    isBetween(predId, self.getNodeId(), successor.getNodeId())) {

                successor = predOfSucc;
            }


            out.println("NOTIFY_PREDECESSOR");
            out.println(self.getNodeId());

        } catch (Exception ignored) {}
    }

    private boolean isBetween(int id, int start, int end) {

        if (start < end) {
            return id > start && id < end;
        } else {
            return id > start || id < end;
        }
    }



}