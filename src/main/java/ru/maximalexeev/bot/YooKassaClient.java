package ru.maximalexeev.bot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

public class YooKassaClient {
    private static final String API_BASE = "https://api.yookassa.ru/v3";
    private final AppConfig config;
    private final HttpClient http;
    private final ObjectMapper om = new ObjectMapper();

    public record CreatedPayment(String id, String status, String confirmationUrl) {}
    public record PaymentInfo(String id, String status, boolean paid) {}

    public YooKassaClient(AppConfig config) {
        this.config = config;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();
    }

    public CreatedPayment createAudioPayment(long chatId, String description, Validation.Contact contact, String fullName) throws Exception {
        String idemKey = UUID.randomUUID().toString();

        ObjectNode root = om.createObjectNode();

        ObjectNode amount = root.putObject("amount");
        amount.put("value", config.audioPriceRub().setScale(2).toPlainString());
        amount.put("currency", "RUB");

        ObjectNode confirmation = root.putObject("confirmation");
        confirmation.put("type", "redirect");
        confirmation.put("return_url", config.yooReturnUrl());

        root.put("capture", true);
        root.put("description", description);

        ObjectNode metadata = root.putObject("metadata");
        metadata.put("telegram_chat_id", String.valueOf(chatId));
        metadata.put("product", "audio_guide_male_translator");

        // receipt (данные для чека) — в том же запросе, что и платеж
        ObjectNode receipt = root.putObject("receipt");
        ObjectNode customer = receipt.putObject("customer");

        if (fullName != null && !fullName.isBlank()) {
            customer.put("full_name", fullName);
        }

        // В зависимости от сценария у кассы может быть email или phone.
        // Мы поддерживаем оба: передаем то, что ввел пользователь.
        if (contact.type() == Validation.ContactType.EMAIL) {
            customer.put("email", contact.value());
        } else {
            // в документации пример с phone для онлайн-кассы
            customer.put("phone", contact.value());
        }

        ArrayNode items = receipt.putArray("items");
        ObjectNode item = items.addObject();
        item.put("description", "Аудио-гид \"Мужской переводчик\"");
        item.put("quantity", 1.000);

        ObjectNode itemAmount = item.putObject("amount");
        itemAmount.put("value", config.audioPriceRub().setScale(2).toPlainString());
        itemAmount.put("currency", "RUB");

        item.put("vat_code", config.receiptVatCode());
        item.put("payment_mode", config.receiptPaymentMode());
        item.put("payment_subject", config.receiptPaymentSubject());

        receipt.put("internet", "true");
        if (config.receiptTimezone() != null) {
            receipt.put("timezone", config.receiptTimezone());
        }

        String body = om.writeValueAsString(root);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/payments"))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", basicAuth(config.yooShopId(), config.yooSecretKey()))
                .header("Idempotence-Key", idemKey)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("YooKassa create payment failed: HTTP " + resp.statusCode() + " body=" + resp.body());
        }

        JsonNode json = om.readTree(resp.body());
        String id = json.path("id").asText();
        String status = json.path("status").asText();
        String confirmationUrl = json.path("confirmation").path("confirmation_url").asText(null);

        return new CreatedPayment(id, status, confirmationUrl);
    }

    public PaymentInfo getPayment(String paymentId) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(API_BASE + "/payments/" + paymentId))
                .timeout(Duration.ofSeconds(20))
                .header("Authorization", basicAuth(config.yooShopId(), config.yooSecretKey()))
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("YooKassa get payment failed: HTTP " + resp.statusCode() + " body=" + resp.body());
        }

        JsonNode json = om.readTree(resp.body());
        String id = json.path("id").asText();
        String status = json.path("status").asText();
        boolean paid = json.path("paid").asBoolean(false);
        return new PaymentInfo(id, status, paid);
    }

    private static String basicAuth(String shopId, String secretKey) {
        String raw = shopId + ":" + secretKey;
        String b64 = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        return "Basic " + b64;
    }
}