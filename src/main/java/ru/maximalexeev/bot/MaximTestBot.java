package ru.maximalexeev.bot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.commands.SetMyCommands;
import org.telegram.telegrambots.meta.api.methods.send.*;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.*;
import org.telegram.telegrambots.meta.api.objects.commands.BotCommand;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import ru.maximalexeev.bot.db.MediaCacheRepository;
import ru.maximalexeev.bot.db.PaymentRepository;
import ru.maximalexeev.bot.db.UserRepository;
import ru.maximalexeev.bot.db.models.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MaximTestBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(MaximTestBot.class);

    private final AppConfig config;
    private final UserRepository userRepo;
    private final PaymentRepository paymentRepo;
    private final MediaCacheRepository mediaCacheRepo;
    private final YooKassaClient yoo;
    private final ScheduledExecutorService scheduler;

    private final PaymentWatcher paymentWatcher;

    public MaximTestBot(AppConfig config,
                        UserRepository userRepo,
                        PaymentRepository paymentRepo,
                        MediaCacheRepository mediaCacheRepo,
                        YooKassaClient yoo) {
        super(config.botToken());
        this.config = config;
        this.userRepo = userRepo;
        this.paymentRepo = paymentRepo;
        this.mediaCacheRepo = mediaCacheRepo;
        this.yoo = yoo;

        this.scheduler = Executors.newScheduledThreadPool(4);

        this.paymentWatcher = (yoo != null)
                ? new PaymentWatcher(scheduler, yoo, paymentRepo)
                : null;

        try {
            // команды (не кнопки) — просто удобство
            execute(new SetMyCommands(List.of(
                    new BotCommand("/start", "Начать тест"),
                    new BotCommand("/admin", "Админ-панель")
            ), null, null));
        } catch (Exception ignored) {}
    }

    @Override
    public String getBotUsername() {
        return config.botUsername();
    }

    public void shutdown() {
        try {
            scheduler.shutdownNow();
        } catch (Exception ignored) {}
    }

    @Override
    public void onUpdateReceived(Update update) {
        try {
            if (update.hasCallbackQuery()) {
                onCallback(update.getCallbackQuery());
                return;
            }
            if (update.hasMessage()) {
                onMessage(update.getMessage());
            }
        } catch (Exception e) {
            log.error("Update handling failed: {}", e.toString(), e);
        }
    }

    private void onMessage(Message msg) throws Exception {
        if (!msg.hasText()) return;

        long chatId = msg.getChatId();
        User tgUser = msg.getFrom();
        if (tgUser == null) return;

        userRepo.upsertUser(chatId, tgUser);
        UserRepository.UserRow u = userRepo.get(chatId);

        String text = msg.getText().trim();

        if (text.equals("/start") || text.startsWith("/start@")) {
            userRepo.resetForStart(chatId);
            sendWelcome(chatId);
            return;
        }

        // админка
        if (text.equals("/admin") || text.startsWith("/admin@")) {
            if (!config.isAdmin(tgUser.getId())) {
                sendText(chatId, "⛔️ Нет доступа.");
                return;
            }
            userRepo.setState(chatId, UserState.ADMIN_MENU);
            sendAdminMenu(chatId);
            return;
        }

        // состояние пользователя
        if (u == null) return;

        if (u.state() == UserState.AWAITING_RECEIPT_CONTACT) {
            handleReceiptContactInput(chatId, tgUser, text);
            return;
        }

        if (u.state() == UserState.ADMIN_BROADCAST_WAIT_TEXT) {
            if (!config.isAdmin(tgUser.getId())) {
                sendText(chatId, "⛔️ Нет доступа.");
                userRepo.setState(chatId, UserState.IDLE);
                return;
            }
            doBroadcast(chatId, text);
            userRepo.setState(chatId, UserState.ADMIN_MENU);
            sendAdminMenu(chatId);
            return;
        }

        // дефолт
        sendText(chatId, "Напиши /start чтобы начать тест 🙂");
    }

    private void onCallback(CallbackQuery cq) throws Exception {
        String data = cq.getData();
        long chatId = cq.getMessage().getChatId();
        long userId = cq.getFrom().getId();

        userRepo.upsertUser(chatId, cq.getFrom());
        UserRepository.UserRow u = userRepo.get(chatId);

        if (data == null) return;

        // QUIZ
        if (data.equals("quiz:go")) {
            userRepo.startQuiz(chatId);
            editOrSendQuestion(cq, 1);
            answerCb(cq, "Поехали 🚀");
            return;
        }

        if (data.startsWith("quiz:ans:")) {
            // quiz:ans:<q>:<A|B|V>
            String[] parts = data.split(":");
            int q = Integer.parseInt(parts[2]);
            String opt = parts[3];

            if (u == null || u.state() != UserState.IN_TEST) {
                answerCb(cq, "Тест не запущен. Нажми /start");
                return;
            }

            if (u.questionIndex() != q) {
                answerCb(cq, "Этот вопрос уже отвечен 🙂");
                return;
            }

            int add = switch (opt) {
                case "A" -> 0;
                case "B" -> 1;
                case "V" -> 2;
                default -> 0;
            };

            int newScore = u.score() + add;

            if (q < 10) {
                userRepo.updateQuizProgress(chatId, q + 1, newScore);
                editOrSendQuestion(cq, q + 1);
                answerCb(cq, "Принято ✅");
            } else {
                // финал
                QuizResult res = QuizContent.calcResult(newScore);
                userRepo.finishQuiz(chatId, res, newScore);
                editOrSendResult(cq, res, newScore);
                answerCb(cq, "Готово ✅");
            }
            return;
        }

        // PDF кнопки
        if (data.startsWith("pdf:")) {
            String key = data.substring("pdf:".length());
            QuizResult r = QuizResult.valueOf(key);

            sendPdfForResult(chatId, r);

            // спустя 10 секунд — апселл (если не спамить)
            if (userRepo.shouldSendUpsell(chatId, 6 * 60 * 60 * 1000L)) { // раз в 6 часов
                userRepo.markUpsellSentNow(chatId);
                scheduler.schedule(() -> {
                    try {
                        sendUpsell(chatId);
                    } catch (Exception e) {
                        log.warn("upsell send failed: {}", e.toString());
                    }
                }, 10, java.util.concurrent.TimeUnit.SECONDS);
            }

            answerCb(cq, "Отправляю файл 📎");
            return;
        }

        // покупка аудио
        if (data.equals("audio:buy")) {
            if (yoo == null) {
                sendText(chatId, "⚠️ Оплата сейчас недоступна (не настроены ключи кассы). Напишите администратору.");
                answerCb(cq, "Оплата недоступна");
                return;
            }
            userRepo.setState(chatId, UserState.AWAITING_RECEIPT_CONTACT);

            InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                    InlineKeyboards.cb("❌ Отмена", "audio:cancel")
            ));

            sendHtml(chatId, """
                    🧾 <b>Для отправки чека</b> напишите <b>email</b> или <b>номер телефона</b> (только цифры, можно с +).

                    Пример:
                    • email: test@example.com
                    • телефон: +79001234567
                    """, kb);

            answerCb(cq, "Введите контакт для чека");
            return;
        }

        if (data.equals("audio:cancel")) {
            userRepo.setState(chatId, UserState.IDLE);
            sendText(chatId, "Ок 🙂");
            answerCb(cq, "Отменено");
            return;
        }

        // проверка оплаты
        if (data.startsWith("pay:check:")) {
            String paymentId = data.substring("pay:check:".length());
            handleCheckPayment(chatId, paymentId);
            answerCb(cq, "Проверяю оплату...");
            return;
        }

        // админка callbacks
        if (data.equals("admin:menu")) {
            if (!config.isAdmin(userId)) {
                answerCb(cq, "Нет доступа");
                return;
            }
            userRepo.setState(chatId, UserState.ADMIN_MENU);
            editOrSendAdminMenu(cq);
            answerCb(cq, "Меню");
            return;
        }

        if (data.equals("admin:stats")) {
            if (!config.isAdmin(userId)) { answerCb(cq, "Нет доступа"); return; }
            sendAdminStats(chatId);
            answerCb(cq, "Ок");
            return;
        }

        if (data.equals("admin:broadcast")) {
            if (!config.isAdmin(userId)) { answerCb(cq, "Нет доступа"); return; }
            userRepo.setState(chatId, UserState.ADMIN_BROADCAST_WAIT_TEXT);
            sendHtml(chatId, """
                    📨 <b>Рассылка</b>

                    Отправьте следующим сообщением текст, который нужно разослать всем пользователям.
                    """, InlineKeyboards.oneColumn(List.of(
                    InlineKeyboards.cb("⬅️ Назад", "admin:menu")
            )));
            answerCb(cq, "Жду текст рассылки");
            return;
        }

        if (data.equals("admin:export")) {
            if (!config.isAdmin(userId)) { answerCb(cq, "Нет доступа"); return; }
            sendUsersCsv(chatId);
            answerCb(cq, "Экспорт");
            return;
        }

        // fallback
        answerCb(cq, "Ок");
    }

    // =========================
    // QUIZ UI
    // =========================

    private void sendWelcome(long chatId) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("ПОГНАЛИ 🚀", "quiz:go")
        ));
        SendMessage sm = new SendMessage();
        sm.setChatId(chatId);
        sm.setText(QuizContent.welcomeText());
        sm.setReplyMarkup(kb);
        execute(sm);
    }

    private InlineKeyboardMarkup answerKeyboard(int qIndex) {
        return InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("А", "quiz:ans:" + qIndex + ":A"),
                InlineKeyboards.cb("Б", "quiz:ans:" + qIndex + ":B"),
                InlineKeyboards.cb("В", "quiz:ans:" + qIndex + ":V")
        ));
    }

    private void editOrSendQuestion(CallbackQuery cq, int qIndex) throws TelegramApiException {
        String text = QuizContent.QUESTIONS.get(qIndex - 1).text();

        EditMessageText em = new EditMessageText();
        em.setChatId(cq.getMessage().getChatId());
        em.setMessageId(cq.getMessage().getMessageId());
        em.setText(text);
        em.setParseMode(ParseMode.HTML);
        em.setReplyMarkup(answerKeyboard(qIndex));

        try {
            execute(em);
        } catch (TelegramApiException e) {
            // fallback: send new message
            sendHtml(cq.getMessage().getChatId(), text, answerKeyboard(qIndex));
        }
    }

    private void editOrSendResult(CallbackQuery cq, QuizResult res, int score) throws TelegramApiException {
        String text = QuizContent.resultText(res) + "\n\n<b>Ваши баллы:</b> " + score;

        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("📎 Получить файл", "pdf:" + res.name())
        ));

        EditMessageText em = new EditMessageText();
        em.setChatId(cq.getMessage().getChatId());
        em.setMessageId(cq.getMessage().getMessageId());
        em.setText(text);
        em.setParseMode(ParseMode.HTML);
        em.setReplyMarkup(kb);

        try {
            execute(em);
        } catch (TelegramApiException e) {
            sendHtml(cq.getMessage().getChatId(), text, kb);
        }
    }

    // =========================
    // PDF sending + caching
    // =========================

    private void sendPdfForResult(long chatId, QuizResult res) throws Exception {
        String fileName = switch (res) {
            case RISK -> config.pdfRisk();
            case NEIGHBORS -> config.pdfNeighbors();
            case ALLIES -> config.pdfAllies();
        };

        Path path = config.mediaDir().resolve(fileName);
        if (!Files.exists(path)) {
            sendText(chatId, "⚠️ Файл не найден в папке media: " + fileName);
            return;
        }

        String cacheKey = "pdf:" + fileName;
        String cachedFileId = mediaCacheRepo.getFileId(cacheKey);

        SendDocument sd = new SendDocument();
        sd.setChatId(chatId);
        sd.setCaption("📎 Ваш PDF готов");
        sd.setParseMode(ParseMode.HTML);

        if (cachedFileId != null) {
            sd.setDocument(new org.telegram.telegrambots.meta.api.objects.InputFile(cachedFileId));
            execute(sd);
            return;
        }

        sd.setDocument(new org.telegram.telegrambots.meta.api.objects.InputFile(path.toFile(), fileName));
        Message m = execute(sd);

        if (m != null && m.getDocument() != null && m.getDocument().getFileId() != null) {
            mediaCacheRepo.putFileId(cacheKey, m.getDocument().getFileId());
        }
    }

    private void sendUpsell(long chatId) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("🔥СКАЧАТЬ АУДИО-ГИД", "audio:buy")
        ));
        sendHtml(chatId, QuizContent.upsellText(), kb);
    }

    // =========================
    // Payment flow
    // =========================

    private void handleReceiptContactInput(long chatId, User tgUser, String text) throws Exception {
        Validation.Contact contact = Validation.parseContact(text);
        if (contact == null) {
            sendHtml(chatId, """
                    ⚠️ Не похоже на email или телефон.

                    Напишите <b>email</b> (test@example.com) или <b>телефон</b> (+79001234567).
                    """, InlineKeyboards.oneColumn(List.of(
                    InlineKeyboards.cb("❌ Отмена", "audio:cancel")
            )));
            return;
        }

        userRepo.setReceiptContact(chatId, contact.value());

        String fullName = ((tgUser.getFirstName() == null ? "" : tgUser.getFirstName()) + " " +
                (tgUser.getLastName() == null ? "" : tgUser.getLastName())).trim();

        String description = "Аудио-гид \"Мужской переводчик\"";

        YooKassaClient.CreatedPayment created = yoo.createAudioPayment(chatId, description, contact, fullName);

        paymentRepo.create(
                created.id(),
                chatId,
                config.audioPriceRub().setScale(2).toPlainString(),
                PaymentStatus.PENDING,
                created.confirmationUrl(),
                contact.value()
        );

        userRepo.setState(chatId, UserState.PAYMENT_PENDING);

        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.url("💳 ОПЛАТИТЬ 490 ₽", created.confirmationUrl()),
                InlineKeyboards.cb("✅ ПРОВЕРИТЬ ОПЛАТУ", "pay:check:" + created.id()),
                InlineKeyboards.cb("❌ Отмена", "audio:cancel")
        ));

        sendHtml(chatId, """
                ✅ Отлично!

                1) Нажмите <b>«ОПЛАТИТЬ 490 ₽»</b> и завершите оплату.
                2) Затем нажмите <b>«ПРОВЕРИТЬ ОПЛАТУ»</b> — и я отправлю файл 🎬
                """, kb);

        // Авто-проверка (чтобы выдать mp4 без лишних кликов)
        if (paymentWatcher != null) {
            paymentWatcher.watch(created.id(), paymentId -> deliverAudio(chatId, paymentId));
        }
    }

    private void handleCheckPayment(long chatId, String paymentId) throws Exception {
        if (yoo == null) {
            sendText(chatId, "⚠️ Оплата не настроена.");
            return;
        }

        PaymentRepository.PaymentRow row = paymentRepo.get(paymentId);
        if (row == null) {
            sendText(chatId, "⚠️ Платеж не найден.");
            return;
        }

        if (row.delivered()) {
            sendText(chatId, "✅ Этот файл уже был выдан.");
            return;
        }

        YooKassaClient.PaymentInfo info = yoo.getPayment(paymentId);
        PaymentStatus st = switch (info.status()) {
            case "pending" -> PaymentStatus.PENDING;
            case "succeeded" -> PaymentStatus.SUCCEEDED;
            case "canceled" -> PaymentStatus.CANCELED;
            default -> PaymentStatus.UNKNOWN;
        };

        paymentRepo.updateStatus(paymentId, st);

        if (st == PaymentStatus.SUCCEEDED && info.paid()) {
            deliverAudio(chatId, paymentId);
            return;
        }

        if (st == PaymentStatus.CANCELED) {
            sendText(chatId, "❌ Платеж отменен или не был завершен.");
            return;
        }

        sendText(chatId, "⏳ Оплата еще не прошла. Попробуйте проверить через минуту.");
    }

    private void deliverAudio(long chatId, String paymentId) throws Exception {
        PaymentRepository.PaymentRow row = paymentRepo.get(paymentId);
        if (row == null) return;
        if (row.delivered()) return;

        Path path = config.mediaDir().resolve(config.audioMp4());
        if (!Files.exists(path)) {
            sendText(chatId, "⚠️ Файл 1.mp4 не найден в папке media.");
            return;
        }

        String cacheKey = "mp4:" + config.audioMp4();
        String cachedFileId = mediaCacheRepo.getFileId(cacheKey);

        SendVideo sv = new SendVideo();
        sv.setChatId(chatId);
        sv.setCaption("🎬 Готово! Вот ваш аудио-гид (файл) ✅");
        sv.setParseMode(ParseMode.HTML);

        Message m;
        if (cachedFileId != null) {
            sv.setVideo(new org.telegram.telegrambots.meta.api.objects.InputFile(cachedFileId));
            m = execute(sv);
        } else {
            sv.setVideo(new org.telegram.telegrambots.meta.api.objects.InputFile(path.toFile(), config.audioMp4()));
            m = execute(sv);
            if (m != null && m.getVideo() != null && m.getVideo().getFileId() != null) {
                mediaCacheRepo.putFileId(cacheKey, m.getVideo().getFileId());
            }
        }

        paymentRepo.markDelivered(paymentId);
        userRepo.setState(chatId, UserState.IDLE);
    }

    // =========================
    // Admin panel
    // =========================

    private void sendAdminMenu(long chatId) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("📊 Статистика", "admin:stats"),
                InlineKeyboards.cb("📨 Рассылка", "admin:broadcast"),
                InlineKeyboards.cb("📤 Экспорт CSV", "admin:export")
        ));
        sendHtml(chatId, "<b>Админ-панель</b>", kb);
    }

    private void editOrSendAdminMenu(CallbackQuery cq) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("📊 Статистика", "admin:stats"),
                InlineKeyboards.cb("📨 Рассылка", "admin:broadcast"),
                InlineKeyboards.cb("📤 Экспорт CSV", "admin:export")
        ));
        EditMessageText em = new EditMessageText();
        em.setChatId(cq.getMessage().getChatId());
        em.setMessageId(cq.getMessage().getMessageId());
        em.setText("<b>Админ-панель</b>");
        em.setParseMode(ParseMode.HTML);
        em.setReplyMarkup(kb);

        try {
            execute(em);
        } catch (TelegramApiException e) {
            sendHtml(cq.getMessage().getChatId(), "<b>Админ-панель</b>", kb);
        }
    }

    private void sendAdminStats(long chatId) throws Exception {
        long users = userRepo.countUsers();
        long finished = userRepo.countFinished();
        long pay = paymentRepo.countSucceeded();

        sendHtml(chatId, """
                📊 <b>Статистика</b>

                👥 Пользователей: <b>%d</b>
                ✅ Завершили тест: <b>%d</b>
                💳 Успешных оплат: <b>%d</b>
                """.formatted(users, finished, pay), InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("⬅️ Назад", "admin:menu")
        )));
    }

    private void doBroadcast(long adminChatId, String text) throws Exception {
        long[] ids = userRepo.listAllChatIds();
        int ok = 0;
        int fail = 0;

        for (long chatId : ids) {
            try {
                SendMessage sm = new SendMessage();
                sm.setChatId(chatId);
                sm.setText(text);
                execute(sm);
                ok++;
            } catch (Exception e) {
                fail++;
            }
        }

        sendText(adminChatId, "📨 Рассылка завершена. Успешно: " + ok + ", ошибок: " + fail);
    }

    private void sendUsersCsv(long chatId) throws Exception {
        // простой CSV: chat_id
        long[] ids = userRepo.listAllChatIds();
        StringBuilder sb = new StringBuilder();
        sb.append("chat_id\n");
        for (long id : ids) sb.append(id).append("\n");

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        SendDocument sd = new SendDocument();
        sd.setChatId(chatId);
        sd.setCaption("📤 users.csv");
        sd.setDocument(new org.telegram.telegrambots.meta.api.objects.InputFile(
                new ByteArrayInputStream(bytes), "users.csv"
        ));

        execute(sd);
    }

    // =========================
    // Helpers
    // =========================

    private void sendText(long chatId, String text) throws TelegramApiException {
        SendMessage sm = new SendMessage();
        sm.setChatId(chatId);
        sm.setText(text);
        execute(sm);
    }

    private void sendHtml(long chatId, String html, InlineKeyboardMarkup kb) throws TelegramApiException {
        SendMessage sm = new SendMessage();
        sm.setChatId(chatId);
        sm.setText(html);
        sm.setParseMode(ParseMode.HTML);
        sm.setReplyMarkup(kb);
        execute(sm);
    }

    private void answerCb(CallbackQuery cq, String text) {
        try {
            AnswerCallbackQuery a = new AnswerCallbackQuery();
            a.setCallbackQueryId(cq.getId());
            a.setText(text);
            a.setShowAlert(false);
            execute(a);
        } catch (Exception ignored) {}
    }
}