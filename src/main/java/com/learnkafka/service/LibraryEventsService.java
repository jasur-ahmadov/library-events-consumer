package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsService {

    private final LibraryEventRepository repository;
    private final ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException, IllegalArgumentException {

        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent in service: {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case CREATE -> save(libraryEvent);
            case UPDATE -> update(libraryEvent);
            default -> log.info("Invalid library event type");
        }
    }

    private void save(LibraryEvent libraryEvent) {

        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        repository.save(libraryEvent);
        log.info("library event saved, {}", libraryEvent);
    }

    private void update(LibraryEvent libraryEvent) throws IllegalArgumentException {

        Optional.ofNullable(libraryEvent.getLibraryEventId())
                .flatMap(repository::findById)
                .ifPresentOrElse(
                        existingEvent -> {
                            log.info("Validation is successful, {}", existingEvent);
                            save(libraryEvent);
                        },
                        () -> {
                            throw new IllegalArgumentException(libraryEvent.getLibraryEventId() == null ? "Library event id is missing" : "Not a valid library event");
                        }
                );
    }
}