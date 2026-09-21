package com.banking.transaction_service.repository;

import com.banking.transaction_service.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TransactionRepository extends JpaRepository<Transaction,String> {

    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String senderAccountNumber);
}
