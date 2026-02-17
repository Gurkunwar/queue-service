package com.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryPriorityQueueService implements QueueService{
    private final Map<String, PriorityQueue<QueueMetaData>> queues;
    private final AtomicLong seqTracker = new AtomicLong(0);
    private long visibilityTimeout;

    InMemoryPriorityQueueService() {
        this.queues = new ConcurrentHashMap<>();
        String propFileName = "config.properties";
        Properties properties = new Properties();

        try (InputStream inStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {
            properties.load(inStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.visibilityTimeout = Integer.parseInt(properties
                .getProperty("visibilityTimeout", "30"));
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        PriorityQueue<QueueMetaData> queue = queues.get(queueUrl);
        if (queue == null) {
            queue = new PriorityQueue<>();
            queues.put(queueUrl, queue);
        }

        int priority = 5;
        String finalBody = messageBody;

        if (messageBody != null && messageBody.matches("^P\\d+:.*")) {
            int colonIndex = messageBody.indexOf(":");
            priority = Integer.parseInt(messageBody.substring(1, colonIndex));
            finalBody = messageBody.substring(colonIndex + 1);
        }

        Message msg = new Message(finalBody);
        QueueMetaData metaData = new QueueMetaData(msg, priority, seqTracker.getAndIncrement());
        queue.add(metaData);
    }

    @Override
    public Message pull(String queueUrl) {
        PriorityQueue<QueueMetaData> queue = queues.get(queueUrl);
        if (queue == null || queue.isEmpty()) {
            return null;
        }

        synchronized (queue) {
            long now = System.currentTimeMillis();

            QueueMetaData[] items = queue.toArray(new QueueMetaData[0]);
            Arrays.sort(items);

            for(QueueMetaData meta : items) {
                Message msg = meta.message;
                if (msg.isVisibleAt(now)) {
                    msg.setReceiptId(UUID.randomUUID().toString());
                    msg.incrementAttempts();
                    msg.setVisibleFrom(now + TimeUnit.SECONDS.toMillis(visibilityTimeout));

                    return new Message(msg.getBody(), msg.getReceiptId());
                }
            }
        }

        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        PriorityQueue<QueueMetaData> queue = queues.get(queueUrl);
        if (queue == null || receiptId == null) return;

        synchronized (queue) {
            queue.removeIf(meta -> receiptId.equals(meta.message.getReceiptId()));
        }
    }

    private static class QueueMetaData implements Comparable<QueueMetaData> {
        final Message message;
        final int priority;
        final long seqNumber;

        QueueMetaData(Message message, int priority, long sequenceNumber) {
            this.message = message;
            this.priority = priority;
            this.seqNumber = sequenceNumber;
        }

        @Override
        public int compareTo(QueueMetaData o) {
            int result = Integer.compare(this.priority, o.priority);
            if(result == 0) {
                return Long.compare(this.seqNumber, o.seqNumber);
            }
            return result;
        }
    }
}