package com.banking.transaction_service.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class TransactionInititatedEvent {

    private String transactionId;
    private String senderAccountNumber;
    private String receiverAccountNumber;
    private BigDecimal amount;
    private String description;
}
