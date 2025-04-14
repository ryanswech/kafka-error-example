package com.example.kafka.error.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.error.services.kafka.data.CrashableRequest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@RestController
@RequestMapping("/api")
public class Controller
{
    private KafkaTemplate<String, CrashableRequest> kafkaTemplate;

    @Autowired
    public Controller(
        KafkaTemplate<String, CrashableRequest> kafkaTemplate
    )
    {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Operation(
        description = "Sends a request to the Kafka topic 'exampleCrashingTopic' whose only listener is setup to throw an OutOfMemoryError. Used to test Kafka's handling of Java Errors.",
        parameters = {
            @Parameter(name = "requestID", description = "An identifier for the request, this will be printed in the logs"),
            @Parameter(name = "shouldCrash", required = false, description = "Whether the listener should crash when it receives this message, defaults to true")
        },
        responses = @ApiResponse(responseCode = "200", description = "The request was sent to Kafka")
    )
    @PostMapping("/crash-topic/send")
    public void sendMessageToCrashTopic(@RequestParam("requestID") int requestID, @RequestParam(value = "shouldCrash", required = false) Boolean shouldCrash)
    {
        if (shouldCrash == null)
            shouldCrash = true;

        kafkaTemplate.send("exampleCrashingTopic", new CrashableRequest(requestID, shouldCrash));
    }
}
