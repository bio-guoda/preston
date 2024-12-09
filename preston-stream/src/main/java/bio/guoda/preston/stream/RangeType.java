package bio.guoda.preston.stream;

public enum RangeType {

    Page("p", "0-9IVXC", ContentStreamFactory.URI_PREFIX_PAGE),
    Line("L", "0-9", ContentStreamFactory.URI_PREFIX_LINE);

    private final String pattern;
    private final String prefix;
    private final String iriPrefix;

    RangeType(String prefix, String pattern, String iriPrefix) {
        this.prefix = prefix;
        this.pattern = pattern;
        this.iriPrefix = iriPrefix;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getPattern() {
        return pattern;
    }

    public String getIriPrefix() {
        return iriPrefix;
    }

}
