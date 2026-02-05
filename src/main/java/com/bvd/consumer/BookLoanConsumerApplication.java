package com.bvd.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class BookLoanConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BookLoanConsumerApplication.class, args);
    }

}
