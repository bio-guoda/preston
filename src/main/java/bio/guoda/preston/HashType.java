package bio.guoda.preston;

public enum HashType {
    SHA256("sha256"),
    LTSH("ltsh");


    private final String name;

    HashType(String name) {
        this.name = name;
    }
}
