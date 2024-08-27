package com.rajan.service;

import com.rajan.TxnCompletedPayload;
import com.rajan.TxnInitPayload;
import com.rajan.WalletUpdatedPayload;
import com.rajan.entity.Wallet;
import com.rajan.repo.WalletRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class WalletService {

    private static Logger LOGGER = LoggerFactory.getLogger(WalletService.class);

    @Autowired
    private WalletRepo walletRepo;

    @Value("${txn.completed.topic}")
    private String txnCompletedTopic;

    @Value("${wallet.updated.topic}")
    private String walletUpdatedTopic;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public void walletTxn(TxnInitPayload txnInitPayload) throws ExecutionException, InterruptedException {

        Wallet fromWallet = walletRepo.findByUserId(txnInitPayload.getFromUserId());
        TxnCompletedPayload txnCompletedPayload = TxnCompletedPayload.builder()
                .id(txnInitPayload.getId())
                .requestId(txnInitPayload.getRequestId())
                .build();

        if (fromWallet.getBalance() < txnInitPayload.getAmount()){
            txnCompletedPayload.setSuccess(false);
            txnCompletedPayload.setReason("Low Balance");
        }
        else {
            Wallet toWallet = walletRepo.findByUserId(txnInitPayload.getToUserId());
            fromWallet.setBalance(fromWallet.getBalance() - txnInitPayload.getAmount());
            toWallet.setBalance(toWallet.getBalance() + txnInitPayload.getAmount());
            txnCompletedPayload.setSuccess(true);

            WalletUpdatedPayload walletUpdatedPayload1 = WalletUpdatedPayload.builder()
                    .userEmail(fromWallet.getUserEmail())
                    .balance(fromWallet.getBalance())
                    .requestId(txnInitPayload.getRequestId())
                    .build();

            WalletUpdatedPayload walletUpdatedPayload2 = WalletUpdatedPayload.builder()
                    .userEmail(toWallet.getUserEmail())
                    .balance(toWallet.getBalance())
                    .requestId(txnInitPayload.getRequestId())
                    .build();

            Future<SendResult<String,Object>> walletUpdatedFuture1  = kafkaTemplate.send(walletUpdatedTopic,walletUpdatedPayload1.getUserEmail(),walletUpdatedPayload1);
            LOGGER.info("Pushed WalletUpdated to kafka: {}",walletUpdatedFuture1.get());

            Future<SendResult<String,Object>> walletUpdatedFuture2  = kafkaTemplate.send(walletUpdatedTopic,walletUpdatedPayload2.getUserEmail(),walletUpdatedPayload2);
            LOGGER.info("Pushed WalletUpdated to kafka: {}",walletUpdatedFuture2.get());
        }
        LOGGER.info("txnCompletedPayload before sending to Kafka: {}", txnCompletedPayload);
        Future<SendResult<String, Object>> future = kafkaTemplate.send(txnCompletedTopic, txnInitPayload.getFromUserId().toString(), txnCompletedPayload);
        LOGGER.info("Pushed TxnCompleted to kafka : {}", future.get());

    }
}
