package com.rajan.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class TxnStatusDto {
    private String status;
    public String reason;
}
