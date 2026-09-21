package com.banking.fraud_detection_service.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
public class FraudCheckResult {

    private boolean fraud;
    private String reason;
}
