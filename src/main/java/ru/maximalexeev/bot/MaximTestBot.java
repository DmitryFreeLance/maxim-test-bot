package ru.maximalexeev.bot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.api.methods.AnswerPreCheckoutQuery;
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.commands.SetMyCommands;
import org.telegram.telegrambots.meta.api.methods.invoices.SendInvoice;
import org.telegram.telegrambots.meta.api.methods.send.SendDocument;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendAudio;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.*;
import org.telegram.telegrambots.meta.api.objects.commands.BotCommand;
import org.telegram.telegrambots.meta.api.objects.payments.LabeledPrice;
import org.telegram.telegrambots.meta.api.objects.payments.OrderInfo;
import org.telegram.telegrambots.meta.api.objects.payments.PreCheckoutQuery;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import ru.maximalexeev.bot.db.MediaCacheRepository;
import ru.maximalexeev.bot.db.PaymentRepository;
import ru.maximalexeev.bot.db.UserRepository;
import ru.maximalexeev.bot.db.models.PaymentStatus;
import ru.maximalexeev.bot.db.models.QuizResult;
import ru.maximalexeev.bot.db.models.UserState;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MaximTestBot extends TelegramLongPollingBot {
    private static final Logger log = LoggerFactory.getLogger(MaximTestBot.class);

    private final AppConfig config;
    private final UserRepository userRepo;
    private final PaymentRepository paymentRepo;
    private final MediaCacheRepository mediaCacheRepo;

    private final ScheduledExecutorService scheduler;

    public MaximTestBot(AppConfig config,
                        UserRepository userRepo,
                        PaymentRepository paymentRepo,
                        MediaCacheRepository mediaCacheRepo) {
        super(config.botToken());
        this.config = config;
        this.userRepo = userRepo;
        this.paymentRepo = paymentRepo;
        this.mediaCacheRepo = mediaCacheRepo;

        this.scheduler = Executors.newScheduledThreadPool(2);

        try {
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
        try { scheduler.shutdownNow(); } catch (Exception ignored) {}
    }

    @Override
    public void onUpdateReceived(Update update) {
        try {
            if (update.hasPreCheckoutQuery()) {
                onPreCheckout(update.getPreCheckoutQuery());
                return;
            }

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
        long chatId = msg.getChatId();
        User tgUser = msg.getFrom();
        if (tgUser == null) return;

        userRepo.upsertUser(chatId, tgUser);
        UserRepository.UserRow u = userRepo.get(chatId);

        // Успешная оплата приходит как message.successful_payment
        if (msg.getSuccessfulPayment() != null) {
            handleSuccessfulPayment(chatId, msg);
            return;
        }

        if (!msg.hasText()) return;
        String text = msg.getText().trim();

        // /start или /start <param>
        if (text.equals("/start") || text.startsWith("/start@") || text.startsWith("/start ")) {
            String param = extractStartParam(text);
            if (param != null && param.equals(config.startParamAudio())) {
                // диплинк -> сразу оффер
                sendAudioOffer(chatId);
                return;
            }

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

        // дефолт
        if (u == null) return;
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

        sendText(chatId, "Напиши /start чтобы начать тест 🙂");
    }

    private String extractStartParam(String text) {
        // варианты:
        // "/start 2"
        // "/start@MyBot 2"
        String[] parts = text.split("\\s+");
        if (parts.length < 2) return null;
        return parts[1].trim();
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
                QuizResult res = QuizContent.calcResult(newScore);
                userRepo.finishQuiz(chatId, res, newScore);
                editOrSendResult(cq, res, newScore);
                answerCb(cq, "Готово ✅");
            }
            return;
        }

        // PDF
        if (data.startsWith("pdf:")) {
            String key = data.substring("pdf:".length());
            QuizResult r = QuizResult.valueOf(key);

            sendPdfForResult(chatId, r);

            answerCb(cq, "Отправляю файл 📎");
            return;
        }

        // Оффер аудио (из диплинка или вручную)
        if (data.equals("audio:offer")) {
            sendAudioOffer(chatId);
            answerCb(cq, "Ок");
            return;
        }

        // Кнопка "Скачать аудио гид" -> отправляем invoice
        if (data.equals("audio:invoice")) {
            if (!config.paymentsEnabled()) {
                sendText(chatId, "⚠️ Оплата сейчас недоступна (не настроен YOOKASSA_PROVIDER_TOKEN).");
                answerCb(cq, "Оплата недоступна");
                return;
            }
            sendAudioInvoice(chatId);
            answerCb(cq, "Счет отправлен");
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

        answerCb(cq, "Ок");
    }

    // =========================
    // Payments (Telegram Invoice)
    // =========================

    private void sendAudioOffer(long chatId) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.cb("Скачать аудио гид", "audio:invoice")
        ));

        // текст ровно как вы просили (можно оставить в QuizContent.upsellText())
        sendHtml(chatId, """
                ✨ Сделайте шаг к совершенству ✨

                🎧 Послушайте аудио-гид "Мужской переводчик" и получите тонкую настройку вашей системы понимания.

                💞 Это инструмент для тех, кто не останавливается на достигнутом и хочет построить по-настоящему крепкую связь.

                Стоимость 490р
                """, kb);
    }

    private void sendAudioInvoice(long chatId) throws Exception {
        // payload = наш идентификатор заказа (будем хранить его как payment_id)
        String payload = "audio_guide:" + chatId + ":" + UUID.randomUUID();

        int priceKopeks = config.audioPriceRub().movePointRight(2).intValueExact();

        SendInvoice inv = new SendInvoice();
        inv.setChatId(chatId);
        inv.setTitle("Аудио-гид «Мужской переводчик»");
        inv.setDescription("Доступ к пакету из 5 аудиофайлов.");
        inv.setPayload(payload);
        inv.setProviderToken(config.yooProviderToken());
        inv.setCurrency("RUB");
        inv.setPrices(List.of(new LabeledPrice("Аудио-гид", priceKopeks)));

        // чтобы получить email/телефон для чека прямо в Telegram
        inv.setNeedEmail(true);
        inv.setNeedPhoneNumber(true);
        inv.setSendEmailToProvider(true);
        inv.setSendPhoneNumberToProvider(true);

        // сохраняем платеж как pending
        paymentRepo.create(
                payload,
                chatId,
                config.audioPriceRub().setScale(2).toPlainString(),
                PaymentStatus.PENDING,
                null,
                null
        );
        userRepo.setState(chatId, UserState.PAYMENT_PENDING);

        execute(inv);
    }

    private void onPreCheckout(PreCheckoutQuery pcq) {
        try {
            String payload = pcq.getInvoicePayload();
            boolean ok = false;
            String error = null;

            // проверим что такой payload есть в БД
            try {
                var row = paymentRepo.get(payload);
                ok = (row != null && !row.delivered());
                if (!ok) error = "Платеж не найден или уже обработан.";
            } catch (Exception e) {
                ok = false;
                error = "Ошибка проверки платежа. Попробуйте еще раз.";
            }

            AnswerPreCheckoutQuery ans = new AnswerPreCheckoutQuery();
            ans.setPreCheckoutQueryId(pcq.getId());
            ans.setOk(ok);
            if (!ok && error != null) ans.setErrorMessage(error);
            execute(ans);
        } catch (Exception e) {
            log.warn("pre_checkout handling failed: {}", e.toString());
        }
    }

    private void handleSuccessfulPayment(long chatId, Message msg) throws Exception {
        var sp = msg.getSuccessfulPayment();
        if (sp == null) return;

        String payload = sp.getInvoicePayload();
        var row = paymentRepo.get(payload);
        if (row == null) {
            // на всякий случай
            sendText(chatId, "⚠️ Платеж получен, но не найден в базе. Напишите администратору.");
            return;
        }
        if (row.delivered()) {
            // уже выдали
            return;
        }

        paymentRepo.updateStatus(payload, PaymentStatus.SUCCEEDED);

        // сохраним контакт из OrderInfo (email/phone)
        String receiptContact = null;
        OrderInfo oi = sp.getOrderInfo();
        if (oi != null) {
            if (oi.getEmail() != null && !oi.getEmail().isBlank()) receiptContact = oi.getEmail();
            else if (oi.getPhoneNumber() != null && !oi.getPhoneNumber().isBlank()) receiptContact = oi.getPhoneNumber();
        }
        if (receiptContact != null) {
            paymentRepo.updateReceiptContact(payload, receiptContact);
            userRepo.setReceiptContact(chatId, receiptContact);
        }

        // выдаем 5 аудио
        deliverAudioBundle(chatId, payload);
    }

    private void deliverAudioBundle(long chatId, String paymentId) throws Exception {
        var row = paymentRepo.get(paymentId);
        if (row == null || row.delivered()) return;

        // отправляем 5 файлов подряд
        for (String fileName : config.audioFiles()) {
            Path path = config.mediaDir().resolve(fileName);
            if (!Files.exists(path)) {
                sendText(chatId, "⚠️ Аудио-файл не найден в папке media: " + fileName);
                continue;
            }

            String cacheKey = "audio:" + fileName;
            String cachedFileId = mediaCacheRepo.getFileId(cacheKey);

            SendAudio sa = new SendAudio();
            sa.setChatId(chatId);
            sa.setCaption(fileName);

            Message m;
            if (cachedFileId != null) {
                sa.setAudio(new org.telegram.telegrambots.meta.api.objects.InputFile(cachedFileId));
                m = execute(sa);
            } else {
                sa.setAudio(new org.telegram.telegrambots.meta.api.objects.InputFile(path.toFile(), fileName));
                m = execute(sa);
                if (m != null && m.getAudio() != null && m.getAudio().getFileId() != null) {
                    mediaCacheRepo.putFileId(cacheKey, m.getAudio().getFileId());
                }
            }
        }

        paymentRepo.markDelivered(paymentId);
        userRepo.setState(chatId, UserState.IDLE);

        sendText(chatId, "✅ Оплата прошла! Я отправил(а) вам 5 аудиофайлов.");
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

    private void sendDeepLinkAfterPdf(long chatId) throws TelegramApiException {
        InlineKeyboardMarkup kb = InlineKeyboards.oneColumn(List.of(
                InlineKeyboards.url("🎧 Получить аудио-гид", config.audioDeepLink())
        ));
        sendHtml(chatId, """
                ✅ PDF получен.

                Хотите продолжение? Откройте оффер по кнопке ниже:
                """, kb);
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
        long[] ids = userRepo.listAllChatIds();
        StringBuilder sb = new StringBuilder();
        sb.append("chat_id\n");
        for (long id : ids) sb.append(id).append("\n");

        byte[] bytes = sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

        org.telegram.telegrambots.meta.api.methods.send.SendDocument sd = new org.telegram.telegrambots.meta.api.methods.send.SendDocument();
        sd.setChatId(chatId);
        sd.setCaption("📤 users.csv");
        sd.setDocument(new org.telegram.telegrambots.meta.api.objects.InputFile(
                new java.io.ByteArrayInputStream(bytes), "users.csv"
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