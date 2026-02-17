package com.example;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class InMemoryPriorityQueueTest {
    private QueueService qs;
    private String queueUrl = "https://sqs.ap-1.amazonaws.com/007/MyQueue";

    @Before
    public void setup() {
        qs = new InMemoryPriorityQueueService();
    }

    @Test
    public void testBasicPushPull(){
        qs.push(queueUrl, "P1:Important Data");
        Message msg = qs.pull(queueUrl);

        assertNotNull(msg);
        assertEquals("Important Data", msg.getBody());
    }

    @Test
    public void testPriorityOrdering() {
        qs.push(queueUrl, "P5:Medium Priority");
        qs.push(queueUrl, "P1:Top Priority");
        qs.push(queueUrl, "P10:Low Priority");

        assertEquals("Top Priority", qs.pull(queueUrl).getBody());
        assertEquals("Medium Priority", qs.pull(queueUrl).getBody());
        assertEquals("Low Priority", qs.pull(queueUrl).getBody());
    }

    @Test
    public void testFCFSWithSamePriority() {
        qs.push(queueUrl, "P2:First Arrival");
        qs.push(queueUrl, "P2:Second Arrival");
        qs.push(queueUrl, "P2:Third Arrival");

        assertEquals("First Arrival", qs.pull(queueUrl).getBody());
        assertEquals("Second Arrival", qs.pull(queueUrl).getBody());
        assertEquals("Third Arrival", qs.pull(queueUrl).getBody());
    }

    @Test
    public void testVisibilityTimeout() {
        qs.push(queueUrl, "P1:Hidden Message");

        Message firstPull = qs.pull(queueUrl);
        assertNotNull(firstPull);

        Message secondPull = qs.pull(queueUrl);
        assertNull("Message should be invisible after being pulled", secondPull);
    }

    @Test
    public void testDeleteMessage() {
        qs.push(queueUrl, "P1:Delete Me");
        Message msg = qs.pull(queueUrl);
        assertNotNull(msg);

        qs.delete(queueUrl, msg.getReceiptId());
        assertNull("Queue should be empty after deletion", qs.pull(queueUrl));
    }

    @Test
    public void testDefaultPriorityHandling() {
        qs.push(queueUrl, "P1:Highest");
        qs.push(queueUrl, "No Prefix Message");
        qs.push(queueUrl, "P10:Lowest");

        assertEquals("Highest", qs.pull(queueUrl).getBody());
        assertEquals("No Prefix Message", qs.pull(queueUrl).getBody());
        assertEquals("Lowest", qs.pull(queueUrl).getBody());
    }
}
