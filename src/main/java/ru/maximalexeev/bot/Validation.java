package ru.maximalexeev.bot;

import java.util.regex.Pattern;

public class Validation {
    private static final Pattern EMAIL = Pattern.compile("^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PHONE = Pattern.compile("^\\+?\\d{10,15}$");

    public enum ContactType { EMAIL, PHONE }

    public record Contact(ContactType type, String value) {}

    public static Contact parseContact(String s) {
        if (s == null) return null;
        String v = s.trim();
        if (EMAIL.matcher(v).matches()) return new Contact(ContactType.EMAIL, v);
        String phone = v.replaceAll("[\\s\\-()]", "");
        if (PHONE.matcher(phone).matches()) return new Contact(ContactType.PHONE, phone.startsWith("+") ? phone.substring(1) : phone);
        return null;
    }
}