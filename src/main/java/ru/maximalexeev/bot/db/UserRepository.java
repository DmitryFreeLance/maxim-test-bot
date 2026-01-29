package ru.maximalexeev.bot.db;

import org.telegram.telegrambots.meta.api.objects.User;
import ru.maximalexeev.bot.db.models.QuizResult;
import ru.maximalexeev.bot.db.models.UserState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class UserRepository {
    private final Database db;

    public record UserRow(
            long chatId,
            long userId,
            String username,
            String firstName,
            String lastName,
            UserState state,
            int questionIndex,
            int score,
            QuizResult lastResult,
            String receiptContact,

            Long upsellSentAt,
            Long quizFinishedAt,

            Long audioPurchasedAt,
            Long systemPurchasedAt,

            Long systemOffer5mSentAt,
            Long followupAudio24hSentAt,
            Long followupSystem24hSentAt
    ) {}

    public record Candidate(long chatId, long ts) {}

    public UserRepository(Database db) {
        this.db = db;
    }

    public UserRow upsertUser(long chatId, User tgUser) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO users (chat_id, user_id, username, first_name, last_name, state, question_index, score, last_result, receipt_contact,
                                   upsell_sent_at, quiz_finished_at, audio_purchased_at, system_purchased_at, system_offer_5m_sent_at, followup_audio_24h_sent_at, followup_system_24h_sent_at,
                                   created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                        ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                  user_id=excluded.user_id,
                  username=excluded.username,
                  first_name=excluded.first_name,
                  last_name=excluded.last_name,
                  updated_at=excluded.updated_at
                """)) {
                ps.setLong(1, chatId);
                ps.setLong(2, tgUser.getId());
                ps.setString(3, tgUser.getUserName());
                ps.setString(4, tgUser.getFirstName());
                ps.setString(5, tgUser.getLastName());
                ps.setString(6, UserState.IDLE.name());
                ps.setLong(7, now);
                ps.setLong(8, now);
                ps.executeUpdate();
            }
        }
        return get(chatId);
    }

    public UserRow get(long chatId) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT * FROM users WHERE chat_id=?")) {
            ps.setLong(1, chatId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return map(rs);
            }
        }
    }

    public void resetForStart(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET state=?, question_index=0, score=0, last_result=NULL, receipt_contact=NULL,
                     upsell_sent_at=NULL, quiz_finished_at=NULL,
                     followup_audio_24h_sent_at=NULL,
                     updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setString(1, UserState.IDLE.name());
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void startQuiz(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET state=?, question_index=1, score=0, last_result=NULL,
                     quiz_finished_at=NULL,
                     upsell_sent_at=NULL,
                     followup_audio_24h_sent_at=NULL,
                     updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setString(1, UserState.IN_TEST.name());
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void setState(long chatId, UserState state) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE users SET state=?, updated_at=? WHERE chat_id=?")) {
            ps.setString(1, state.name());
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void setReceiptContact(long chatId, String contact) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE users SET receipt_contact=?, updated_at=? WHERE chat_id=?")) {
            ps.setString(1, contact);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void updateQuizProgress(long chatId, int nextQuestionIndex, int newScore) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET question_index=?, score=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setInt(1, nextQuestionIndex);
            ps.setInt(2, newScore);
            ps.setLong(3, now);
            ps.setLong(4, chatId);
            ps.executeUpdate();
        }
    }

    public void finishQuiz(long chatId, QuizResult result, int finalScore) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET state=?, question_index=0, score=?, last_result=?, quiz_finished_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setString(1, UserState.IDLE.name());
            ps.setInt(2, finalScore);
            ps.setString(3, result.name());
            ps.setLong(4, now);
            ps.setLong(5, now);
            ps.setLong(6, chatId);
            ps.executeUpdate();
        }
    }

    public void markUpsellSentNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("UPDATE users SET upsell_sent_at=?, updated_at=? WHERE chat_id=?")) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void markAudioPurchasedNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET audio_purchased_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void markSystemPurchasedNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET system_purchased_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void markSystemOffer5mSentNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET system_offer_5m_sent_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void markFollowupAudio24hSentNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET followup_audio_24h_sent_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    public void markFollowupSystem24hSentNow(long chatId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 UPDATE users
                 SET followup_system_24h_sent_at=?, updated_at=?
                 WHERE chat_id=?
                 """)) {
            ps.setLong(1, now);
            ps.setLong(2, now);
            ps.setLong(3, chatId);
            ps.executeUpdate();
        }
    }

    // ---- кандидаты: 5 минут после аудио, если не купили систему
    public List<Candidate> listSystemOffer5mCandidates(long cutoffMs) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 SELECT chat_id, audio_purchased_at
                 FROM users
                 WHERE audio_purchased_at IS NOT NULL
                   AND system_purchased_at IS NULL
                   AND system_offer_5m_sent_at IS NULL
                   AND state = ?
                   AND audio_purchased_at <= ?
                 """)) {
            ps.setString(1, UserState.IDLE.name());
            ps.setLong(2, cutoffMs);
            return readCandidates(ps);
        }
    }

    // ---- кандидаты: 24 часа после теста, если НЕ купили аудио
    public List<Candidate> listFollowupAudio24hCandidates(long cutoffMs) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 SELECT chat_id, quiz_finished_at
                 FROM users
                 WHERE quiz_finished_at IS NOT NULL
                   AND audio_purchased_at IS NULL
                   AND followup_audio_24h_sent_at IS NULL
                   AND state = ?
                   AND quiz_finished_at <= ?
                 """)) {
            ps.setString(1, UserState.IDLE.name());
            ps.setLong(2, cutoffMs);
            return readCandidates(ps);
        }
    }

    // ---- кандидаты: 24 часа после аудио, если НЕ купили систему
    public List<Candidate> listFollowupSystem24hCandidates(long cutoffMs) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 SELECT chat_id, audio_purchased_at
                 FROM users
                 WHERE audio_purchased_at IS NOT NULL
                   AND system_purchased_at IS NULL
                   AND followup_system_24h_sent_at IS NULL
                   AND state = ?
                   AND audio_purchased_at <= ?
                 """)) {
            ps.setString(1, UserState.IDLE.name());
            ps.setLong(2, cutoffMs);
            return readCandidates(ps);
        }
    }

    private List<Candidate> readCandidates(PreparedStatement ps) throws Exception {
        List<Candidate> res = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                long chatId = rs.getLong("chat_id");
                long ts = rs.getLong(2);
                res.add(new Candidate(chatId, ts));
            }
        }
        return res;
    }

    public long countUsers() throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) AS c FROM users");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("c") : 0;
        }
    }

    public long countFinished() throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) AS c FROM users WHERE last_result IS NOT NULL");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("c") : 0;
        }
    }

    public long[] listAllChatIds() throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT chat_id FROM users");
             ResultSet rs = ps.executeQuery()) {
            ArrayList<Long> ids = new ArrayList<>();
            while (rs.next()) ids.add(rs.getLong("chat_id"));
            long[] arr = new long[ids.size()];
            for (int i = 0; i < ids.size(); i++) arr[i] = ids.get(i);
            return arr;
        }
    }

    private UserRow map(ResultSet rs) throws Exception {
        long chatId = rs.getLong("chat_id");
        long userId = rs.getLong("user_id");
        String username = rs.getString("username");
        String firstName = rs.getString("first_name");
        String lastName = rs.getString("last_name");
        UserState state = UserState.valueOf(rs.getString("state"));
        int q = rs.getInt("question_index");
        int score = rs.getInt("score");
        String lr = rs.getString("last_result");
        QuizResult lastResult = (lr == null) ? null : QuizResult.valueOf(lr);
        String contact = rs.getString("receipt_contact");

        Long upsellSentAt = getNullableLong(rs, "upsell_sent_at");
        Long quizFinishedAt = getNullableLong(rs, "quiz_finished_at");

        Long audioPurchasedAt = getNullableLong(rs, "audio_purchased_at");
        Long systemPurchasedAt = getNullableLong(rs, "system_purchased_at");

        Long systemOffer5mSentAt = getNullableLong(rs, "system_offer_5m_sent_at");
        Long followupAudio24hSentAt = getNullableLong(rs, "followup_audio_24h_sent_at");
        Long followupSystem24hSentAt = getNullableLong(rs, "followup_system_24h_sent_at");

        return new UserRow(
                chatId, userId, username, firstName, lastName, state, q, score, lastResult, contact,
                upsellSentAt, quizFinishedAt,
                audioPurchasedAt, systemPurchasedAt,
                systemOffer5mSentAt, followupAudio24hSentAt, followupSystem24hSentAt
        );
    }

    private Long getNullableLong(ResultSet rs, String col) throws Exception {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
}