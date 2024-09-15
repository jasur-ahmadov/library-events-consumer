package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailureService {

    private final FailureRecordRepository repository;

    public void saveFailedRecords(ConsumerRecord<Integer, String> record, Exception ex, String status) {

        var failureRecord = new FailureRecord(null,
                record.topic(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset(),
                ex.getCause().getMessage(),
                status);

        repository.save(failureRecord);
    }
}