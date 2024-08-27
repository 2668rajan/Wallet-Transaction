package com.rajan;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletUpdatedPayload {
    private String userEmail;

    private Double balance;

    private String requestId;
}
