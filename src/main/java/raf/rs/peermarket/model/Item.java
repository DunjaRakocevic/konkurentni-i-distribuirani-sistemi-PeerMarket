package raf.rs.peermarket.model;

import java.io.Serializable;

public class Item implements Serializable {

    private final int itemId;
    private final String name;
    private int quantity;
    private final int ownerId;

    public Item(int itemId, String name, int quantity, int ownerId) {
        this.itemId = itemId;
        this.name = name;
        this.quantity = quantity;
        this.ownerId = ownerId;
    }

    public int getItemId() {
        return itemId;
    }

    public String getName() {
        return name;
    }

    public int getQuantity() {
        return quantity;
    }

    public void decrease(int amount) {
        this.quantity -= amount;
    }

    public int getOwnerId() {
        return ownerId;
    }


}