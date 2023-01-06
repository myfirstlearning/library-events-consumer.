package com.libraryeventsconsumer.consumer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.libraryeventsconsumer.entity.Book;
import com.libraryeventsconsumer.entity.LibraryEvent;
import com.libraryeventsconsumer.entity.LibraryEventType;
import com.libraryeventsconsumer.repository.LibraryEventsRepository;
import com.libraryeventsconsumer.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
//Override bootstrap server properties with embedded kafka
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    //@Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    //@Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumer;

    @SpyBean
    LibraryEventsService libraryEventsService;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for(MessageListenerContainer messageListenerContainer: endpointRegistry.getAllListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\":\"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka using springboot\",\n" +
                "        \"bookAuthor\": \"Thompson\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);


        Mockito.verify(libraryEventsConsumer, Mockito.times(1)).onMessage(isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(456, libraryEvent.getBook().getBookId());
        });

    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\":\"NEW\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka using springboot\",\n" +
                "        \"bookAuthor\": \"Thompson\"\n" +
                "    }\n" +
                "}";

        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        Mockito.verify(libraryEventsConsumer, Mockito.times(1)).onMessage(isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());

    }

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n" +
                "    \"libraryEventId\": null,\n" +
                "    \"libraryEventType\":\"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka using springboot\",\n" +
                "        \"bookAuthor\": \"Thompson\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        Mockito.verify(libraryEventsConsumer, Mockito.times(1)).onMessage(isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsService, Mockito.times(1)).processLibraryEvent(isA(ConsumerRecord.class));

    }

    @Test
    void publishUpdateLibraryEvent_DataAccessException() throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\n" +
                "    \"libraryEventId\": 999,\n" +
                "    \"libraryEventType\":\"UPDATE\",\n" +
                "    \"book\": {\n" +
                "        \"bookId\": 456,\n" +
                "        \"bookName\": \"Kafka using springboot\",\n" +
                "        \"bookAuthor\": \"Thompson\"\n" +
                "    }\n" +
                "}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        Mockito.verify(libraryEventsConsumer, Mockito.times(3)).onMessage(isA(ConsumerRecord.class));
        Mockito.verify(libraryEventsService, Mockito.times(3)).processLibraryEvent(isA(ConsumerRecord.class));

    }
}
