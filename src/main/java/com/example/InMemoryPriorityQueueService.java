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
        PriorityQueue<QueueMetaData> queue = queues.computeIfAbsent(queueUrl, k -> new PriorityQueue<>());

        int priority = parsePriority(messageBody);
        String finalBody = cleanBody(messageBody);

        QueueMetaData metaData = new QueueMetaData(new Message(finalBody), priority, seqTracker.getAndIncrement());

        synchronized (queue) {
            queue.add(metaData);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        PriorityQueue<QueueMetaData> queue = queues.get(queueUrl);
        if (queue == null || queue.isEmpty()) {
            return null;
        }

        synchronized (queue) {
            if (queue.isEmpty()) return null;

            long now = System.currentTimeMillis();
            List<QueueMetaData> skipped = new ArrayList<>();
            QueueMetaData selected = null;

            while (!queue.isEmpty()) {
                QueueMetaData meta = queue.poll();
                if (meta.message.isVisibleAt(now)) {
                    selected = meta;
                    break;
                } else {
                    skipped.add(meta);
                }
            }

            queue.addAll(skipped);

            if (selected != null) {
                Message msg = selected.message;
                msg.setReceiptId(UUID.randomUUID().toString());
                msg.incrementAttempts();
                msg.setVisibleFrom(now + TimeUnit.SECONDS.toMillis(visibilityTimeout));

                queue.add(selected);
                return msg;
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

    private int parsePriority(String body) {
        if (body != null && body.matches("^P\\d+:.*")) {
            return Integer.parseInt(body.substring(1, body.indexOf(":")));
        }
        return 5;
    }

    private String cleanBody(String body) {
        if (body != null && body.matches("^P\\d+:.*")) {
            return body.substring(body.indexOf(":") + 1);
        }
        return body;
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