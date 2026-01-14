package ru.maximalexeev.bot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.maximalexeev.bot.db.PaymentRepository;
import ru.maximalexeev.bot.db.models.PaymentStatus;

import java.util.Map;
import java.util.concurrent.*;

public class PaymentWatcher {
    private static final Logger log = LoggerFactory.getLogger(PaymentWatcher.class);

    private final ScheduledExecutorService scheduler;
    private final YooKassaClient yoo;
    private final PaymentRepository paymentRepo;

    private final Map<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();

    public interface OnPaymentSucceeded {
        void handle(String paymentId) throws Exception;
    }

    public PaymentWatcher(ScheduledExecutorService scheduler, YooKassaClient yoo, PaymentRepository paymentRepo) {
        this.scheduler = scheduler;
        this.yoo = yoo;
        this.paymentRepo = paymentRepo;
    }

    public void watch(String paymentId, OnPaymentSucceeded callback) {
        // если уже есть задача — не плодим
        if (tasks.containsKey(paymentId)) return;

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                var row = paymentRepo.get(paymentId);
                if (row == null) {
                    cancel(paymentId);
                    return;
                }
                if (row.delivered()) {
                    cancel(paymentId);
                    return;
                }

                YooKassaClient.PaymentInfo info = yoo.getPayment(paymentId);

                PaymentStatus st = mapStatus(info.status());
                paymentRepo.updateStatus(paymentId, st);

                if (st == PaymentStatus.SUCCEEDED && info.paid()) {
                    callback.handle(paymentId);
                    cancel(paymentId);
                }
                if (st == PaymentStatus.CANCELED) {
                    cancel(paymentId);
                }
            } catch (Exception e) {
                log.warn("PaymentWatcher tick failed paymentId={}: {}", paymentId, e.toString());
            }
        }, 5, 7, TimeUnit.SECONDS);

        tasks.put(paymentId, future);

        // авто-таймаут: 10 минут
        scheduler.schedule(() -> cancel(paymentId), 10, TimeUnit.MINUTES);
    }

    public void cancel(String paymentId) {
        ScheduledFuture<?> f = tasks.remove(paymentId);
        if (f != null) f.cancel(false);
    }

    private static PaymentStatus mapStatus(String s) {
        if (s == null) return PaymentStatus.UNKNOWN;
        return switch (s) {
            case "pending" -> PaymentStatus.PENDING;
            case "succeeded" -> PaymentStatus.SUCCEEDED;
            case "canceled" -> PaymentStatus.CANCELED;
            default -> PaymentStatus.UNKNOWN;
        };
    }
}