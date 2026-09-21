package com.banking.account_service.dto;

import com.banking.account_service.entity.AccountType;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateAccountRequest {

    @NotBlank(message = "Account holder name is required")
    private String accountHolderName;

    @NotBlank(message = "Email is required" )
    @Email(message = "Invalid email format")
    private String email;

    @NotBlank(message = "phone number is required")
    private String phone;

    @NotNull(message = "Account type is requried")
    private AccountType accountType;

    @NotNull(message = "Intitial deposit is required")
    @Positive(message = "Initial deposit can't be a negative value")
    private BigDecimal initialDeposit;


}
