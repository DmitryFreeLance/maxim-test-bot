package ru.maximalexeev.bot.db.models;

public enum UserState {
    IDLE,
    IN_TEST,

    AWAITING_RECEIPT_CONTACT,   // ждем email или телефон для чека
    PAYMENT_PENDING,            // платеж создан, ждем оплату

    ADMIN_MENU,
    ADMIN_BROADCAST_WAIT_TEXT
}