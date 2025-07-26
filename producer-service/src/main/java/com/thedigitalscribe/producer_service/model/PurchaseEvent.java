package com.thedigitalscribe.producer_service.model;

import lombok.Data;

@Data
public class PurchaseEvent {
    private String orderId;
    private String product;
    private int quantity;
    private double price;
    private String timestamp;
}
