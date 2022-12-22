package bio.guoda.preston.util;

import java.util.regex.Pattern;

public class UUIDUtil {
    public static final Pattern UUID_PATTERN
            = Pattern.compile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
}
