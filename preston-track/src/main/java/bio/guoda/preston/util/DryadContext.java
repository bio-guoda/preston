package bio.guoda.preston.util;

public class DryadContext implements AuthContext {
    private final String token;

    public DryadContext(String token) {
        this.token = token;
    }

    @Override
    public String getAccessToken() {
        return token;
    }
}
