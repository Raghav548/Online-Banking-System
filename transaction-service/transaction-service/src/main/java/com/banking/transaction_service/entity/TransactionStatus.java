package com.banking.transaction_service.entity;

/*** Transaction Lifecycle Flow:
        * PENDING -> PROCESSING -> COMPLETED (clean transaction)
                                -> PENDING_VERIFICATION (suspicious detected)
                                        -> COMPLETED (verified)
                                        -> FLAGGED (SAGA REFUND)

                                -> FLAGGED

                                -> FAILED

*/
public enum TransactionStatus {

    PENDING,
    PROCESSING,
    COMPLETED,
    PENDING_VERIFICATION,
    FLAGGED,
    FAILED
}
