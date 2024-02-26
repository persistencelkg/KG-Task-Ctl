package org.kg.ctl.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Author liuyy
 */
public class DateTimeUtil {

    private DateTimeUtil() {
    }

    public static final String PATTERN_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String PATTERN_YYYY_MM_DD = "yyyy-MM-dd";

    public static final DateTimeFormatter YYYY_MM_DD_HH_MM_SS = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD_HH_MM_SS);
    public static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern(PATTERN_YYYY_MM_DD);

    private static final Pattern T_N_REGX =  Pattern.compile("^T[+-]\\d+(D)?$");

    public static boolean isValid(String str) {
        return T_N_REGX.matcher(str).matches();
    }

    public static void main(String[] args) {
        System.out.println(isValid("T-1h"));
    }

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
