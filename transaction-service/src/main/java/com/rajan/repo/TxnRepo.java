package com.rajan.repo;

import com.rajan.entity.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TxnRepo extends JpaRepository<Transaction, Long> {
    Transaction findByTxnId(String txnId);
}
