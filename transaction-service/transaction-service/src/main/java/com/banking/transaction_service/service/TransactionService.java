package com.banking.transaction_service.service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.TransactionCompletedEvent;
import com.banking.transaction_service.event.TransactionInititatedEvent;

import com.banking.transaction_service.repository.TransactionRepository;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    private final RedisTemplate<String,String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

/*** SAGA STEP - 1: Initiate transfer
        * Deducts from sender via feign
        * Saves transaction as PROCESSING.
        * Publish event to kafka for fraud check
        * Returns.

 */

    public TransactionResponse transfer(TransferRequest request) {

        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // SAGA STEP 1: Deduct from sender
        accountServiceClient.deductBalance(
                request.getSenderAccountNumber(),
                request.getAmount());

        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType. TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        //SAGA step 2 - publish event to kafka for fraud check
        TransactionInititatedEvent event = new TransactionInititatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()

        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(),event);
        log.info("SAGA step 2 - TransactionInitiatedEvent published : {}",savedTransaction.getId());
        return mapToResponse(savedTransaction);

    }

    public TransactionResponse verifyOTP(String transactionID, String otp) {
        log.info("OTP verification for the transaction: {}", transactionID);

        Transaction transaction = transactionRepository.findById(transactionID)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found " + transactionID

                ));

        String otpKey = "verification:otp" + transactionID;
        String stored0tp = redisTemplate.opsForValue().get(otpKey);

        if(stored0tp == null) {
            // OTP EXPIRED
            log.warn("OTP expired for transaction: {}", transactionID);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if(!stored0tp.equals(otp)) {
            // BLOCK ACCOUNT AND REFUND
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionID);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction,
                    "Wrong OTP entered : transaction cancelled, "+
                    "Account blocked for security");
            return mapToResponse(transaction);
        }

        // OTP correct - complete transaction
        log.info("OTP verified - completing transaction: {}", transactionID);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        // CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at " + LocalDateTime.now());

        transactionRepository.save(transaction);

        // PUBLISH refund event - Notification service will alert user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);

        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {

        // Publish fraud.detected -> Account Service will block account
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud. detected published - account: {} will be blocked, Kindly contact to the bank",
                transaction.getSenderAccountNumber());

        // SAGA COMPENSATION - refund Sender
        compensateTransaction(transaction, reason);

    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()

        );

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

        log.info("SAGA COMPLETE - Transaction {} completed",
                transaction.getId());
    }

    public void processCleanResult(String transactionID) {

        Transaction transaction = transactionRepository.findById(transactionID)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found " + transactionID

                ));

        //TODO - understand this idempotency guard
        if (transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction {} not PROCESSING - skipping", transactionID);
            return;
        }
        completeTransaction(transaction);
    }

    public TransactionResponse getTransaction(String transactionId){
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: "+transactionId)));
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        return transactionRepository
                        .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                        .stream()
                        .map(this :: mapToResponse)
                        .collect(Collectors.toList());

    }

    private TransactionResponse mapToResponse(Transaction transaction) {

        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(
                transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(
                transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());

        return response;
    }

}
