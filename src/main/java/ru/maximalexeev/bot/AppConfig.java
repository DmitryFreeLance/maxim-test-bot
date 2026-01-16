package ru.maximalexeev.bot;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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

        // audio bundle (5 files)
        List<String> audioFiles,

        // Telegram Payments (YooKassa provider via BotFather)
        String yooProviderToken,
        BigDecimal audioPriceRub,

        // deep links
        String startParamAudio
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

        String pdfRisk = env("PDF_RISK", "ХОЛОДНАЯ ВОЙНА.pdf");
        String pdfNeighbors = env("PDF_NEIGHBORS", "Как перестать быть соседями.pdf");
        String pdfAllies = env("PDF_ALLIES", "Секреты сильных пар.pdf");

        // Можно переопределять одной переменной:
        // AUDIO_FILES="Стоп-кран.m4a,Система.m4a,Секс и быт.m4a,Пещера.m4a,Инструкция.m4a"
        String audioFilesRaw = env("AUDIO_FILES",
                "Пещера.wav,Стоп-кран.wav,Инструкция.wav,Секс и быт.wav,Система.wav");
        List<String> audioFiles = Arrays.stream(audioFilesRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        BigDecimal price = new BigDecimal(env("AUDIO_PRICE_RUB", "490.00"));

        // Provider token from BotFather -> Payments -> YooKassa
        String providerToken = env("YOOKASSA_PROVIDER_TOKEN", "");

        // deep-link start param for audio offer
        String startParamAudio = env("START_PARAM_AUDIO", "2");

        return new AppConfig(
                token, username, admins,
                dbPath, mediaDir,
                pdfRisk, pdfNeighbors, pdfAllies,
                audioFiles,
                providerToken, price,
                startParamAudio
        );
    }

    public boolean isAdmin(long userId) {
        return adminIds != null && adminIds.contains(userId);
    }

    public boolean paymentsEnabled() {
        return yooProviderToken != null && !yooProviderToken.isBlank();
    }

    public String audioDeepLink() {
        return "https://t.me/" + botUsername + "?start=" + startParamAudio;
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