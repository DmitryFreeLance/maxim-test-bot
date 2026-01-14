package ru.maximalexeev.bot;

import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;
import ru.maximalexeev.bot.db.Database;
import ru.maximalexeev.bot.db.MediaCacheRepository;
import ru.maximalexeev.bot.db.PaymentRepository;
import ru.maximalexeev.bot.db.UserRepository;

public class Main {
    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv();

        Database db = new Database(config.dbPath());
        db.migrate();

        UserRepository userRepo = new UserRepository(db);
        PaymentRepository paymentRepo = new PaymentRepository(db);
        MediaCacheRepository mediaCacheRepo = new MediaCacheRepository(db);

        YooKassaClient yooKassaClient = null;
        if (config.yooKassaEnabled()) {
            yooKassaClient = new YooKassaClient(config);
        }

        MaximTestBot bot = new MaximTestBot(config, userRepo, paymentRepo, mediaCacheRepo, yooKassaClient);

        TelegramBotsApi botsApi = new TelegramBotsApi(DefaultBotSession.class);
        botsApi.registerBot(bot);

        Runtime.getRuntime().addShutdownHook(new Thread(bot::shutdown));
    }
}