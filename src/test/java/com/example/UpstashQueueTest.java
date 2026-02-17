package com.example;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

public class UpstashQueueTest {
    private UpstashQueueService upstashQueueService;
    private final String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        Properties prop = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            prop.load(input);

            String apiUrl = prop.getProperty("upstash.api.url");
            String apiToken = prop.getProperty("upstash.api.token");

            upstashQueueService = new UpstashQueueService(apiUrl, apiToken);

            while (upstashQueueService.pull(queueUrl) != null) {}
        } catch (IOException ex) {
            throw new RuntimeException("Error loading configuration", ex);
        }
    }

    @Test
    public void testPushAndPullSuccess() {
        String messageBody = "Test message body " + System.currentTimeMillis();
        upstashQueueService.push(queueUrl, messageBody);

        Message msg = upstashQueueService.pull(queueUrl);

        assertNotNull("Should have received a message from Upstash", msg);
        assertEquals(messageBody, msg.getBody());
    }

    @Test
    public void testPullEmptyQueue() {
        String emptyQueue = "EmptyQueue-" + System.currentTimeMillis();
        Message msg = upstashQueueService.pull(emptyQueue);

        assertNull("Should be null for an empty Redis list", msg);
    }

    @Test
    public void testPushWithSpecialCharacters() {
        String complexMessage = "Message with \"quotes\", \n newlines, and emojis 🚀";

        upstashQueueService.push(queueUrl, complexMessage);
        Message received = upstashQueueService.pull(queueUrl);

        assertNotNull(received);
        assertEquals(complexMessage, received.getBody());
    }

    @Test(expected = RuntimeException.class)
    public void testNetworkFailureWithInvalidToken() {
        UpstashQueueService badService = new UpstashQueueService(
                "https://example.io", "invalid-token");
        badService.push(queueUrl, "this should fail");
    }

    @Test
    public void testLargePayload() {
        StringBuilder largeMsg = new StringBuilder();
        for(int i=0; i<1000; i++) largeMsg.append("Data Chunk ");

        String body = largeMsg.toString();
        upstashQueueService.push(queueUrl, body);
        Message received = upstashQueueService.pull(queueUrl);

        assertNotNull(received);
        assertEquals(body, received.getBody());
    }

    @Test
    public void testQueueOrderingFIFO() {
        upstashQueueService.push(queueUrl, "First");
        upstashQueueService.push(queueUrl, "Second");
        upstashQueueService.push(queueUrl, "Third");

        assertEquals("First", upstashQueueService.pull(queueUrl).getBody());
        assertEquals("Second", upstashQueueService.pull(queueUrl).getBody());
        assertEquals("Third", upstashQueueService.pull(queueUrl).getBody());
    }

    @Test
    public void testEmptyStringMessage() {
        upstashQueueService.push(queueUrl, "");
        Message msg = upstashQueueService.pull(queueUrl);

        assertNotNull(msg);
        assertEquals("", msg.getBody());
    }
}