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
        BigDecimal systemPriceRub,

        // course link
        String systemMaterialsUrl,

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

        String pdfRisk = env("PDF_RISK", "Холодная война.pdf");
        String pdfNeighbors = env("PDF_NEIGHBORS", "Как перестать быть соседями.pdf");
        String pdfAllies = env("PDF_ALLIES", "Секреты сильных пар.pdf");

        String audioFilesRaw = env("AUDIO_FILES",
                "Пещера.wav,Стоп-кран.wav,Инструкция.wav,Секс и быт.wav,Система.wav");
        List<String> audioFiles = Arrays.stream(audioFilesRaw.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        BigDecimal audioPrice = new BigDecimal(env("AUDIO_PRICE_RUB", "490.00"));
        BigDecimal systemPrice = new BigDecimal(env("SYSTEM_PRICE_RUB", "1990.00"));

        String providerToken = env("YOOKASSA_PROVIDER_TOKEN", "");

        String startParamAudio = env("START_PARAM_AUDIO", "2");

        String materialsUrl = env("SYSTEM_MATERIALS_URL", "https://sistema-soyuzniki-tx3upgy.gamma.site/");

        return new AppConfig(
                token, username, admins,
                dbPath, mediaDir,
                pdfRisk, pdfNeighbors, pdfAllies,
                audioFiles,
                providerToken, audioPrice, systemPrice,
                materialsUrl,
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