package com.banking.fraud_detection_service.client;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.cloud.openfeign.FeignClient;
import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {


        @GetMapping("/api/v1/accounts/{accountNumber}/balance")
        BigDecimal getBalance(
                @PathVariable String accountNumber);


}
