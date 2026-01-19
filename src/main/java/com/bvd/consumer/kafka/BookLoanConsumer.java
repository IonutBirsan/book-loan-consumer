package com.bvd.consumer.kafka;

import com.bvd.consumer.model.BookLoan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class BookLoanConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BookLoanConsumer.class);
    private int messageCount = 0;

    @KafkaListener(topics = "book-loans", groupId = "book-loan-consumer-group")
    public void consumeBookLoan(
            @Payload BookLoan bookLoan,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        messageCount++;

        logger.info("Processing message #{} from partition {} at offset {}",
                messageCount, partition, offset);

        logger.debug("BookLoan details - ID: {}, Member: {}, Book: '{}', Genre: {}, Author: {}, Days: {}",
                bookLoan.getLoanId(),
                bookLoan.getMemberId(),
                bookLoan.getBookTitle(),
                bookLoan.getGenre(),
                bookLoan.getAuthor(),
                bookLoan.getDaysLoaned());

        try {

            processBookLoan(bookLoan);

            logger.info("Successfully processed BookLoan: {} - '{}' by {}",
                    bookLoan.getLoanId(),
                    bookLoan.getBookTitle(),
                    bookLoan.getAuthor());

        } catch (Exception e) {
            logger.error("Failed to process BookLoan {}: {}",
                    bookLoan.getLoanId(),
                    e.getMessage(),
                    e);

        }
    }

    private void processBookLoan(BookLoan bookLoan) {

        logger.debug("Processing loan for member {} - book '{}'",
                bookLoan.getMemberId(),
                bookLoan.getBookTitle());
    }
}