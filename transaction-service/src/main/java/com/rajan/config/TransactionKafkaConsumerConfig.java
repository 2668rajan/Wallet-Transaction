package com.rajan.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rajan.TxnCompletedPayload;
import com.rajan.entity.Transaction;
import com.rajan.enums.TxnStatusEnum;
import com.rajan.repo.TxnRepo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class TransactionKafkaConsumerConfig {

    private static Logger LOGGER = LoggerFactory.getLogger(TransactionKafkaConsumerConfig.class);

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private TxnRepo txnRepo;

    @KafkaListener(topics = "${txn.completed.topic}", groupId = "txn")
    public void consumeTxnCompleted(ConsumerRecord payload) throws JsonProcessingException {
        LOGGER.info("Raw payload: {}", payload.value().toString());
        TxnCompletedPayload txnCompletedPayload = OBJECT_MAPPER.readValue(payload.value().toString(), TxnCompletedPayload.class);
        MDC.put("requestId", txnCompletedPayload.getRequestId());
        LOGGER.info("Read from kafka TxnInit : {}", txnCompletedPayload);
        LOGGER.info("txnCompletedPayload : {}", txnCompletedPayload);
        Transaction transaction = txnRepo.findById(txnCompletedPayload.getId()).get();

        if (transaction == null) {
            LOGGER.error("Transaction not found for ID: {}", txnCompletedPayload.getId());
            return;
        }
        LOGGER.info("transaction : {}", transaction);
        Boolean success = txnCompletedPayload.getSuccess();
        LOGGER.info("Success value: {}", success);

        if(success != null && success){
            transaction.setStatus(TxnStatusEnum.SUCCESS);
        }
        else{
            transaction.setStatus(TxnStatusEnum.FAILED);
            transaction.setReason(txnCompletedPayload.getReason());
        }
        txnRepo.save(transaction);
        MDC.clear();
    }

}
