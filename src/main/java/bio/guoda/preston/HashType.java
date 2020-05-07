package bio.guoda.preston;

public enum HashType {
    SHA256("hash://sha256/"),
    TLSH("hash://tlsh/");


    private final String prefix;

    HashType(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
