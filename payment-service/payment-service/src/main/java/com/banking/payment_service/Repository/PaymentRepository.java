package com.banking.payment_service.Repository;

import com.banking.payment_service.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment,String> {
    Optional<Payment> findByRazorpayOrderId(String razorPayOrderId);
}
