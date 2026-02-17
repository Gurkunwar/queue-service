package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class UpstashQueueService implements QueueService {
    private final String apiUrl;
    private final String apiToken;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public UpstashQueueService(String apiUrl, String apiToken) {
        this.apiUrl = apiUrl;
        this.apiToken = apiToken;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Override
    public void push(String queueUrl, String messageBody) {
        try {
            String[] command = {"RPUSH", queueUrl, messageBody};
            String jsonPayload = objectMapper.writeValueAsString(command);
            executeCommand(jsonPayload);
        } catch (Exception e) {
            throw new RuntimeException("Error encoding Redis command", e);
        }
    }

    @Override
    public Message pull(String queueUrl) {
        try {
            String[] command = {"LPOP", queueUrl};
            String jsonPayload = objectMapper.writeValueAsString(command);
            String response = executeCommand(jsonPayload);

            JsonNode root = objectMapper.readTree(response);
            JsonNode result = root.get("result");

            if (result == null || result.isNull()) {
                return null;
            }

            return new Message(result.asText());
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void delete(String queueUrl, String receiptId) {
        throw new UnsupportedOperationException(
                "Upstash Redis LPOP model does not support manual receipt deletion.");
    }

    protected String executeCommand(String jsonBody) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Authorization", "Bearer " + apiToken)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Upstash error: HTTP " +
                        response.statusCode() + " - " + response.body());
            }

            return response.body();
        } catch (Exception e) {
            throw new RuntimeException("Communication failure with Upstash", e);
        }
    }
}