package dev.wixforth.columnar.schema;

import java.time.LocalDate;

/**
 * Fast conversion utilities between ISO-8601 date strings and epoch-day int32 representations.
 */
public final class DateUtil {
    private DateUtil() {}

    public static int parseEpochDay(String isoDate) {
        return (int) LocalDate.parse(isoDate).toEpochDay();
    }

    public static String formatEpochDay(int epochDay) {
        return LocalDate.ofEpochDay(epochDay).toString();
    }
}
