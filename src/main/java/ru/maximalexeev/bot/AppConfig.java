package ru.maximalexeev.bot;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public record AppConfig(
        String botToken,
        String botUsername,
        Set<Long> adminIds,
        String dbPath,
        Path mediaDir,

        // media names
        String pdfRisk,
        String pdfNeighbors,
        String pdfAllies,
        String audioMp4,

        // payments YooKassa
        String yooShopId,
        String yooSecretKey,
        String yooReturnUrl,
        BigDecimal audioPriceRub,

        // receipt params
        int receiptVatCode,
        String receiptPaymentMode,
        String receiptPaymentSubject,
        Integer receiptTimezone
) {
    public static AppConfig fromEnv() {
        String token = require("BOT_TOKEN");
        String username = require("BOT_USERNAME");

        String adminsRaw = env("ADMIN_IDS", "");
        Set<Long> admins = Arrays.stream(adminsRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(Long::parseLong)
                .collect(Collectors.toSet());

        String dbPath = env("DB_PATH", "/app/data/bot.db");
        Path mediaDir = Path.of(env("MEDIA_DIR", "/app/media"));

        String pdfRisk = env("PDF_RISK", "risk.pdf");
        String pdfNeighbors = env("PDF_NEIGHBORS", "neighbors.pdf");
        String pdfAllies = env("PDF_ALLIES", "allies.pdf");
        String audioMp4 = env("AUDIO_MP4", "1.mp4");

        String shopId = env("YOOKASSA_SHOP_ID", "");
        String secret = env("YOOKASSA_SECRET_KEY", "");
        String returnUrl = env("YOOKASSA_RETURN_URL", "https://t.me/" + username);

        BigDecimal price = new BigDecimal(env("AUDIO_PRICE_RUB", "490.00"));

        int vatCode = Integer.parseInt(env("RECEIPT_VAT_CODE", "1"));
        String paymentMode = env("RECEIPT_PAYMENT_MODE", "full_payment");
        String paymentSubject = env("RECEIPT_PAYMENT_SUBJECT", "service");
        Integer tz = null;
        String tzRaw = env("RECEIPT_TIMEZONE", "2");
        if (!tzRaw.isBlank()) tz = Integer.parseInt(tzRaw);

        return new AppConfig(
                token, username, admins,
                dbPath, mediaDir,
                pdfRisk, pdfNeighbors, pdfAllies, audioMp4,
                shopId, secret, returnUrl, price,
                vatCode, paymentMode, paymentSubject, tz
        );
    }

    public boolean isAdmin(long userId) {
        return adminIds != null && adminIds.contains(userId);
    }

    public boolean yooKassaEnabled() {
        return yooShopId != null && !yooShopId.isBlank() && yooSecretKey != null && !yooSecretKey.isBlank();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null) ? def : v;
    }

    private static String require(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + key);
        }
        return v;
    }
}