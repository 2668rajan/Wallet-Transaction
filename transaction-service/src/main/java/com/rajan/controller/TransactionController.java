package com.rajan.controller;

import com.rajan.dto.TxnRequestDto;
import com.rajan.dto.TxnStatusDto;
import com.rajan.service.TransactionService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/transaction-service")
public class TransactionController {

    private static Logger LOGGER = LoggerFactory.getLogger(TransactionController.class);

    @Autowired
    private TransactionService transactionService;

    @PostMapping("/txn")
    public ResponseEntity<String> intiTransaction(@RequestBody @Valid TxnRequestDto txnRequestDto) throws ExecutionException, InterruptedException {
        LOGGER.info("Starting transaction : {}", txnRequestDto);
        String txnId = transactionService.initTransaction(txnRequestDto);
        return ResponseEntity.accepted().body(txnId);
    }

    @GetMapping("/status/{txnId}")
    public ResponseEntity<TxnStatusDto> getTxnStatus(@PathVariable String txnId){
        return ResponseEntity.ok(transactionService.getStatus(txnId));
    }
}
