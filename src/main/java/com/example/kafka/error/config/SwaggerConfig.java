package com.example.kafka.error.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;

@Configuration
public class SwaggerConfig
{
    @Bean
    public OpenAPI getSwaggerInfo()
    {
        return new OpenAPI();
    }
}
