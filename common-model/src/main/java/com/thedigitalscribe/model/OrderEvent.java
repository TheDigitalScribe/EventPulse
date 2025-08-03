package com.thedigitalscribe.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;

@Data
public class OrderEvent {

    @NotBlank
    private String orderId;

    @NotBlank
    private String product;

    @Min(1)
    private int quantity;

    @DecimalMin("0.01")
    private BigDecimal price;

    @NotNull
    private Instant timestamp;
}
