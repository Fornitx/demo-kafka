package com.example.kafka.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MyDto {
    private UUID id;
    private Long number;
    private String text;
    private Boolean flag;
    private Instant instant;
}
