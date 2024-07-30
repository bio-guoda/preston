package bio.guoda.preston;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class DateUtil {

    private static final DateTimeFormatter DATE_TIME_FORMATTER_UTC
            = ISODateTimeFormat.dateTime().withZoneUTC();

    private static final DateTimeFormatter DATE_FORMATTER_UTC
            = ISODateTimeFormat.date().withZoneUTC();

    public static String now() {
        return DATE_TIME_FORMATTER_UTC.print(new Date().getTime());
    }

    public static String nowDate() {
        return DATE_FORMATTER_UTC.print(new Date().getTime());
    }

    public static DateTime parse(String dateTimeString) {
        return DATE_TIME_FORMATTER_UTC.parseDateTime(dateTimeString);
    }
}
