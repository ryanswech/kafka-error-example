package com.example.kafka.error.services.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.kafka.error.services.kafka.data.CrashableRequest;

@Component
public class Listener
{
    private static final Logger logger = LoggerFactory.getLogger(Listener.class);

    @KafkaListener(
        id = "exampleCrashingListener",
        topics = "exampleCrashingTopic",
        groupId = "exampleGroup",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void exampleCrashingListener(CrashableRequest request)
    {
        logger.info("Pretending to start work for requestID={}", request.getRequestID());

        try
        {
            Thread.sleep(30000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Sleep that emulates work of exampleCrashingListener was inturrupted!");
        }

        if (request.getShouldCrash())
        {
            logger.info("Crashing (throwing OutOfMemoryError) for requestID={}", request.getRequestID());
            throw new OutOfMemoryError();
        }

        logger.info("Work finised for requestID={}", request.getRequestID());
    }
}
