package com.rajan;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
//@JsonIgnoreProperties(ignoreUnknown = true)
public class TxnCompletedPayload {
    private Long id;
    private Boolean success=false;
    private String reason;
    private String requestId;
}
