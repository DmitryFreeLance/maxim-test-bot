package ru.maximalexeev.bot.db;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Database {
    private final String dbPath;

    public Database(String dbPath) {
        this.dbPath = dbPath;
        ensureParentDir();
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
    }

    public void migrate() throws Exception {
        try (Connection c = getConnection(); Statement s = c.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL;");
            s.execute("PRAGMA foreign_keys=ON;");

            s.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                      chat_id INTEGER PRIMARY KEY,
                      user_id INTEGER,
                      username TEXT,
                      first_name TEXT,
                      last_name TEXT,
                      state TEXT NOT NULL,
                      question_index INTEGER NOT NULL DEFAULT 0,
                      score INTEGER NOT NULL DEFAULT 0,
                      last_result TEXT,
                      receipt_contact TEXT,

                      upsell_sent_at INTEGER,
                      quiz_finished_at INTEGER,

                      audio_purchased_at INTEGER,
                      system_purchased_at INTEGER,

                      system_offer_5m_sent_at INTEGER,
                      followup_audio_24h_sent_at INTEGER,
                      followup_system_24h_sent_at INTEGER,

                      system_invoice_5m_sent_at INTEGER,

                      created_at INTEGER NOT NULL,
                      updated_at INTEGER NOT NULL
                    );
                    """);

            s.execute("""
                    CREATE TABLE IF NOT EXISTS media_cache (
                      media_key TEXT PRIMARY KEY,
                      telegram_file_id TEXT NOT NULL,
                      updated_at INTEGER NOT NULL
                    );
                    """);

            s.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                      payment_id TEXT PRIMARY KEY,
                      chat_id INTEGER NOT NULL,
                      amount_value TEXT NOT NULL,
                      status TEXT NOT NULL,
                      confirmation_url TEXT,
                      receipt_contact TEXT,
                      delivered INTEGER NOT NULL DEFAULT 0,
                      created_at INTEGER NOT NULL,
                      updated_at INTEGER NOT NULL
                    );
                    """);

            s.execute("CREATE INDEX IF NOT EXISTS idx_payments_chat_id ON payments(chat_id);");
            s.execute("CREATE INDEX IF NOT EXISTS idx_users_state ON users(state);");

            // ---- миграции для существующей БД (если колонок нет)
            try { s.execute("ALTER TABLE users ADD COLUMN quiz_finished_at INTEGER;"); } catch (Exception ignored) {}
            try { s.execute("ALTER TABLE users ADD COLUMN upsell_sent_at INTEGER;"); } catch (Exception ignored) {}

            try { s.execute("ALTER TABLE users ADD COLUMN audio_purchased_at INTEGER;"); } catch (Exception ignored) {}
            try { s.execute("ALTER TABLE users ADD COLUMN system_purchased_at INTEGER;"); } catch (Exception ignored) {}

            try { s.execute("ALTER TABLE users ADD COLUMN system_offer_5m_sent_at INTEGER;"); } catch (Exception ignored) {}
            try { s.execute("ALTER TABLE users ADD COLUMN followup_audio_24h_sent_at INTEGER;"); } catch (Exception ignored) {}
            try { s.execute("ALTER TABLE users ADD COLUMN followup_system_24h_sent_at INTEGER;"); } catch (Exception ignored) {}

            try { s.execute("ALTER TABLE users ADD COLUMN system_invoice_5m_sent_at INTEGER;"); } catch (Exception ignored) {}
        }
    }

    private void ensureParentDir() {
        try {
            Path p = Path.of(dbPath).toAbsolutePath().getParent();
            if (p != null) Files.createDirectories(p);
        } catch (Exception ignored) { }
    }
}