package org.kg.ctl.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * @author liuyy
 */
public class DateTimeUtil {

    private DateTimeUtil() {
    }

    public static final String PATTERN_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_YYYY_MM_DD = "yyyy-MM-dd";

    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_HH_MM_SS);
    public static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD);

    public static LocalDateTime parse(String dateTimeString) {
        return LocalDateTime.parse(dateTimeString, YYYY_MM_DD_HH_MM_SS);
    }

    public static String format(LocalDateTime localDateTime) {
        if (Objects.isNull(localDateTime)) {
            return null;
        }
        return YYYY_MM_DD_HH_MM_SS.format(localDateTime);
    }

    public static String format(LocalDateTime localDateTime,DateTimeFormatter dateTimeFormatter) {
        if (Objects.isNull(localDateTime) || dateTimeFormatter == null) {
            return null;
        }
        return dateTimeFormatter.format(localDateTime);
    }
}
