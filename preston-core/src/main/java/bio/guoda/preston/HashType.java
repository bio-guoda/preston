package bio.guoda.preston;

public enum HashType {
    sha256("hash://sha256/", "SHA-256"),
    md5("hash://md5/", "MD5");

    private final String prefix;
    private final String algorithm;

    HashType(String prefix, String algorithm) {
        this.prefix = prefix;
        this.algorithm = algorithm;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAlgorithm() {
        return algorithm;
    }
}
