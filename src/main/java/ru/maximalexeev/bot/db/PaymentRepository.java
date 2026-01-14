package ru.maximalexeev.bot.db;

import ru.maximalexeev.bot.db.models.PaymentStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PaymentRepository {
    private final Database db;

    public record PaymentRow(
            String paymentId,
            long chatId,
            String amountValue,
            PaymentStatus status,
            String confirmationUrl,
            String receiptContact,
            boolean delivered
    ) {}

    public PaymentRepository(Database db) {
        this.db = db;
    }

    public void create(String paymentId, long chatId, String amountValue, PaymentStatus status, String confirmationUrl, String receiptContact) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 INSERT INTO payments(payment_id, chat_id, amount_value, status, confirmation_url, receipt_contact, delivered, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
                 """)) {
            ps.setString(1, paymentId);
            ps.setLong(2, chatId);
            ps.setString(3, amountValue);
            ps.setString(4, status.name());
            ps.setString(5, confirmationUrl);
            ps.setString(6, receiptContact);
            ps.setLong(7, now);
            ps.setLong(8, now);
            ps.executeUpdate();
        }
    }

    public PaymentRow get(String paymentId) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT * FROM payments WHERE payment_id=?")) {
            ps.setString(1, paymentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return map(rs);
            }
        }
    }

    public void updateStatus(String paymentId, PaymentStatus status) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE payments SET status=?, updated_at=? WHERE payment_id=?")) {
            ps.setString(1, status.name());
            ps.setLong(2, now);
            ps.setString(3, paymentId);
            ps.executeUpdate();
        }
    }

    public void markDelivered(String paymentId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE payments SET delivered=1, updated_at=? WHERE payment_id=?")) {
            ps.setLong(1, now);
            ps.setString(2, paymentId);
            ps.executeUpdate();
        }
    }

    public long countSucceeded() throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) AS c FROM payments WHERE status=?");
        ) {
            ps.setString(1, PaymentStatus.SUCCEEDED.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("c") : 0;
            }
        }
    }

    private PaymentRow map(ResultSet rs) throws Exception {
        return new PaymentRow(
                rs.getString("payment_id"),
                rs.getLong("chat_id"),
                rs.getString("amount_value"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getString("confirmation_url"),
                rs.getString("receipt_contact"),
                rs.getInt("delivered") == 1
        );
    }
}