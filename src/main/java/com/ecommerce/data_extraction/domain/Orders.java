package com.ecommerce.data_extraction.domain;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.data.relational.core.mapping.Column;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Entity
@Table(name = "orders_v2")
@RequiredArgsConstructor
@Data
public class Orders {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "order_seq_gen")
    @SequenceGenerator(name = "order_seq_gen", sequenceName = "orders_v2_seq", allocationSize = 50)
    private Long id;

    private String invoiceNo;
    private String stockCode;

    private String description;

    private int quantity;
    private Timestamp invoiceDate;
    private BigDecimal unitPrice;
    private Long customerId;
    private String country;

    // Getters and setters omitted for brevity
}
