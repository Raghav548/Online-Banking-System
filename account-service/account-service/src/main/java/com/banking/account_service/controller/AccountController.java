package com.banking.account_service.controller;

import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.service.AccountService;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;

@RestController
@RequestMapping("api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(@RequestBody CreateAccountRequest request){

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(accountService.createAccount(request));
    }

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber){

        return ResponseEntity.ok(accountService.getAccount(accountNumber));
    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> getBalance(@PathVariable String accountNumber){

        return ResponseEntity.ok(accountService.getBalance(accountNumber));
    }


    @PutMapping("/{accountNumber}/block")
    public ResponseEntity<String> blockAccount(@PathVariable String accountNumber) {
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked Successfully");

    }

     /*** SAGA STEP 1 - Deduct Balance
    * Called by Transaction Service when transfer is initiated
    */

    @PutMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> deductBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount) {
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Balance deducted Successfully");
    }

    /*** SAGA STEP 4 - Compensating transaction endpoint
     CALLED BY TRANSACTION SERVICE in TWO SCENARIOS:
                1. Fraud detected -> refund sender (undo step 1)
               2. Transaction completed -> Credit receiver

    **/

    @PutMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(
            @PathVariable String accountNumber,
            @RequestParam BigDecimal amount) {
        accountService.creditBalance(accountNumber, amount);
        return ResponseEntity.ok("BALANCE CREDITED SUCESSFULLY");
    }


}
