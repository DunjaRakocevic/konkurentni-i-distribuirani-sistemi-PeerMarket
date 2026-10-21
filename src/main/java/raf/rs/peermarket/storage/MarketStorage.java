package raf.rs.peermarket.storage;

import raf.rs.peermarket.model.Item;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MarketStorage {



    private final Map<Integer, Item> items = new ConcurrentHashMap<>();



    public void addItem(Item item) {
        items.put(item.getItemId(), item);
    }

    public Item getItem(int id) {
        return items.get(id);
    }

    public Map<Integer, Item> getAll() {
        return items;
    }

    public void removeItem(int itemId) {
        items.remove(itemId);
    }

    public void replaceItem(int itemId, Item newItem) {
        items.put(itemId, newItem);
    }

    public boolean exists(int itemId) {
        return items.containsKey(itemId);
    }

    public void transferOwnership(int itemId, int newOwnerId) {

        Item old = items.get(itemId);
        if (old == null) return;

        Item updated = new Item(
                old.getItemId(),
                old.getName(),
                old.getQuantity(),
                newOwnerId
        );

        items.put(itemId, updated);
    }
}