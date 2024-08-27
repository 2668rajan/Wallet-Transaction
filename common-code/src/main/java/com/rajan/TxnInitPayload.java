package com.rajan;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class TxnInitPayload {
    private Long id;
    private Long fromUserId;
    private Long toUserId;
    private Double amount;
    private String requestId;
}
