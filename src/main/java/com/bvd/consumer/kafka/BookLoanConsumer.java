package com.bvd.consumer.kafka;

import com.bvd.consumer.model.BookLoan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BookLoanConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BookLoanConsumer.class);

    @KafkaListener(topics = "book-loans", groupId = "book-loan-consumer-group")
    public void consumeBookLoan(BookLoan bookLoan) {
        logger.info("========================================");
        logger.info("📚 Received BookLoan: {}", bookLoan);
        logger.info("========================================");
        System.out.println("📚 RECEIVED: " + bookLoan);
    }
}