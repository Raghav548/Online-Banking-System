package com.banking.transaction_service.service;

import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventConsumer {

    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;
    private final RedisTemplate<String, String> redisTemplate;
    private static final long OTP_EXPIRY_MINUTES = 5;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /**
     * COnsume verification.required
     * Generate OTP and ask user to verify
     */

    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(@Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required - transaction: {} reason: {}",
                    transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId)
                    .orElseThrow(() -> new RuntimeException(
                            "Transaction not found " + transactionId

                    ));

            // TODO - check here we have put idempotency guard why and how is it ?
            if(transaction.getStatus() != TransactionStatus.PROCESSING) {
                log.warn("Transaction {} not PROCESSING - skipping", transactionId);
                return;
            }

            // Generate 6 digit otp
            String otp= String.format("%06d", (int) (Math.random() * 900000) + 100000);

            // Store OTP in Redis - expires in 5 minutes
            String otpKey = "verification:otp" + transactionId;
            redisTemplate.opsForValue().set(otpKey, otp, OTP_EXPIRY_MINUTES, TimeUnit.MINUTES);

            // Update Status
            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction: {} expires in {} min",
                    transactionId, OTP_EXPIRY_MINUTES);

            // Notify user (kafka event for notification service to publish otp)
            Map<String, Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC,transactionId,otpEvent);

// Update Status
        } catch (Exception e) {
            log.error("Error handling verification required : {}",e.getMessage());
        }
    }


    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload){

        try {
            log.info("Processing fraud check clean event");
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);
        }
        catch(Exception e){
                    log.error("Error processing fraud check result: {}",e.getMessage());
        }
    }
}