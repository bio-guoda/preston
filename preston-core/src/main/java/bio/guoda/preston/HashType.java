package bio.guoda.preston;

import java.util.regex.Pattern;

public enum HashType {
    sha256("hash://sha256/", "SHA-256", 64, 78),
    md5("hash://md5/", "MD5", 32, 43),
    sha1("hash://sha1/", "SHA-1", 40, 52);

    private final String prefix;
    private final String algorithm;
    private final int hexLength;
    private final Pattern hexPattern;
    private final Pattern iriPattern;
    private final String iriPatternString;
    private final int iriStringLength;

    HashType(String prefix, String algorithm, int hexLength, int iriStringLength) {
        this.prefix = prefix;
        this.algorithm = algorithm;
        this.hexLength = hexLength;
        String hexPatternString = "([a-fA-F0-9])" + "{" + hexLength +  "}$";
        this.hexPattern = Pattern.compile(hexPatternString);
        iriPatternString = prefix + hexPatternString;
        this.iriPattern = Pattern.compile("(?<prefix>[a-zA-Z0-9]{2,}:)*(?<contentId>" + iriPatternString + "){1}" + "(?<suffix>!/.*){0,1}");
        this.iriStringLength = iriStringLength;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public int getHexLength() {
        return hexLength;
    }

    public Pattern getIRIPattern() {
        return iriPattern;
    }

    public Pattern getHexPattern() {
        return hexPattern;
    }

   public String getIRIPatternString() {
        return iriPatternString;
    }

    public int getIriStringLength() {
        return iriStringLength;
    }
}
