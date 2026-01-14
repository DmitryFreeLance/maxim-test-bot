package ru.maximalexeev.bot.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MediaCacheRepository {
    private final Database db;

    public MediaCacheRepository(Database db) {
        this.db = db;
    }

    public String getFileId(String key) throws Exception {
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT telegram_file_id FROM media_cache WHERE media_key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return rs.getString("telegram_file_id");
            }
        }
    }

    public void putFileId(String key, String fileId) throws Exception {
        long now = System.currentTimeMillis();
        try (Connection c = db.getConnection();
             PreparedStatement ps = c.prepareStatement("""
                 INSERT INTO media_cache(media_key, telegram_file_id, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(media_key) DO UPDATE SET telegram_file_id=excluded.telegram_file_id, updated_at=excluded.updated_at
                 """)) {
            ps.setString(1, key);
            ps.setString(2, fileId);
            ps.setLong(3, now);
            ps.executeUpdate();
        }
    }
}