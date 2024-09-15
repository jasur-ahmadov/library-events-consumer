package com.learnkafka.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer key;
    private String errorMsg;
    private Integer partition;
    private Long offset;
    private String exception;
    private String status;
}