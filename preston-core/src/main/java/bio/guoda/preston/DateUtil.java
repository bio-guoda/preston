package bio.guoda.preston;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class DateUtil {

    public static String now() {
        return ISODateTimeFormat.dateTime().withZoneUTC().print(new Date().getTime());
    }

    public static DateTime parse(String dateTimeString) {
        return ISODateTimeFormat.dateTime().withZoneUTC().parseDateTime(dateTimeString);
    }
}
