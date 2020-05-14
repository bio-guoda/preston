package bio.guoda.preston;

public enum HashType {
    sha256("hash://sha256/"),
    tika_tlsh("hash://tika-tlsh/"),
    tlsh("hash://tlsh/");

    private final String prefix;

    HashType(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
