package com.banking.payment_service.Service;

import com.banking.payment_service.Repository.PaymentRepository;
import com.banking.payment_service.dto.CreatePaymentRequest;
import com.banking.payment_service.dto.PaymentOrderResponse;
import com.banking.payment_service.entity.PaymentStatus;
import com.banking.payment_service.entity.Payment;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private final PaymentRepository paymentRepository;
    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";
    private final KafkaTemplate<String,Object> kafkaTemplate;

    /**Create Razorpay payment order.
     * FLOW:
     1. Create order in razorpay
    ` 2. Save Payment record in DB
    * 3. Return order details to frontend
    * 4. Frontend show Razorpay Checkout
    * 5. User pays
    6. Razorpay calls webhook
    **/

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account: {} amount: {}",
                request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // Converted Amount
        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount", convertedAmount);
        orderRequest.put("currency", "USD/INR");
        orderRequest.put("receipt", "rcpt_" + System.currentTimeMillis() + UUID.randomUUID().toString()
                .replace( "-",  "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razorpay order created: {}", razorpayOrder.get("id").toString());

        // Save payment record
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setAmount(request.getAmount());
        payment.setCurrency("USD/INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment= paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                 "USD/INR",
                 "CREATED",
                keyId
        );
    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook: {}", payload.get("event"));

        String event = (String) payload.get("event");

        if ("payment.captured".equals(event)) {
            handlePayementSuccess(payload);
        } else if ("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }
    }

    private void handlePayementSuccess(Map<String, Object> payload) {
        try {

            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: " + orderId

                    ));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), event);
            log.info("Payment completed: {}", payment.getId());
        } catch (Exception e) {
            log.error("Error handling payment success: {}",e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload){
        try{
            Map<String, Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException(
                            "Payment not found for order: "+orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            // Publish payment completed event
            Map<String, Object> event = new HashMap<>();
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("reason", "payment failed via Razorpay");
            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), event);

            log.warn("Payment failed: {}", payment.getId());

        }

        catch (Exception e) {
            log.error("Error handling payment failure: {}",e.getMessage());
        }
    }

    //TODO check the Razorpay webhook payload - to confirm why we are extractingf like this
    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {
        Map<String, Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");

    }


}
