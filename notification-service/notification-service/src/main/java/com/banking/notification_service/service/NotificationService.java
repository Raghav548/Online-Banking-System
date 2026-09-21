package com.banking.notification_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consume0tpGenerated(@Payload Map<String, Object> payload) {

        try {
            String accountNumber = (String) payload.get("accountNumber");
            String otp = (String) payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. " +
                                    "Reason: %s " +
                                    "A transaction of %s is pending verification. " +
                                    "Your OTP is: %s. Valid for 5 minutes. " +
                                    "If this wasn't you - ignore this message.",
                                    reason,amount,otp

                    )

            );
        }
        catch(Exception e){
            log.error("Error sending notification message : {}",e.getMessage());
        }
        }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload){
        try {

            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // DEBIT ALERT
            sendAlert(senderAccount,
                     "DEBIT ALERT",
                    String.format(
                            "%s debited from account %s",
                            amount, senderAccount

                    ));

            // CREDIT ALERT
            sendAlert(receiverAccount,
                     "CREDIT ALERT",
                    String.format(
                            "%s credited to account %s",
                            amount, receiverAccount
                    ));
        }


        catch (Exception e) {
            log.error("Error sending transaction Notification: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload){
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");


            sendAlert(accountNumber,
                    "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked. ",
                            "Reason: %s. ",
                            "Please Contact your bank Immediately",
                            accountNumber, reason
                    ));
        }
        catch (Exception e) {
            log.error("Error sending Fraud alert: {}",e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(
            @Payload Map<String, Object> payload){
        try {

            String senderAccount = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = (String) payload.get("reason");


            sendAlert(senderAccount,
                    "REFUND PROCESSED",
                    String.format(
                            "Your transaction of %s was cancelled ",
                            "Reason: %s. ",
                            "%s has been refunded to account %s.",
                            amount, reason,amount,senderAccount
                    ));
        }
        catch (Exception e) {
            log.error("Error sending Refund notification: {}",e.getMessage());
        }
    }

    //for Razorpay (external payments)
    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload){
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();
            String razorpayId = (String) payload.get("razorpayPaymentId");

            sendAlert(accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Your payment of %s was completed ",
                            "Razorpay ID: %s. ",
                            amount,razorpayId
                    ));
        }
        catch (Exception e) {
            log.error("Error sending payment successful notification: {}",e.getMessage());
        }
    }

    //for Razorpay (external payments)
    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String, Object> payload){
        try {

            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(accountNumber,
                    "PAYMENT Failed",
                    String.format(
                            "Your payment of %s could not be processed ",
                            "Please try again or contact support. ",
                            amount
                    ));
        }
        catch (Exception e) {
            log.error("Error sending payment Failed notification: {}",e.getMessage());
        }
    }

    //TODO - for now i am just logging up but in future this method can be set to send sms or mail for alerts
    private void sendAlert(String accountNumber, String subject, String message) {

        log.info("----------------------------------------");
        log.info("Account: {}",accountNumber);
        log.info("Subject: {}",subject);
        log.info("message: {}",message);
        log.info("------------------------------------------");
    }
}

