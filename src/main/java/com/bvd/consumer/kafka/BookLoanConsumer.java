package com.bvd.consumer.kafka;

import com.bvd.consumer.model.BookLoan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class BookLoanConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BookLoanConsumer.class);
    private final AtomicLong messageCount = new AtomicLong(0);

    @KafkaListener(topics = "${kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(
            @Payload BookLoan bookLoan,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        long count = messageCount.incrementAndGet();

        logger.info("Processing message #{} from partition {} at offset {}", count, partition, offset);
        logger.info("Successfully processed BookLoan: {} - '{}' by {}",
                bookLoan.getLoanId(),
                bookLoan.getBookTitle(),
                bookLoan.getAuthor());
    }
}