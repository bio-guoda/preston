package bio.guoda.preston.zenodo;

import java.util.UUID;

public class ZenodoContext {

    private final String endpoint;
    private Long depositId;
    private UUID bucketId;
    private final String accessToken;

    public ZenodoContext(String accessToken) {
        this(accessToken, "https://zenodo.org");
    }

    public ZenodoContext(String accessToken, String endpoint) {
        this.accessToken = accessToken;
        this.endpoint = endpoint;
    }


    public String getAccessToken() {
        return accessToken;
    }

    public UUID getBucketId() {
        return bucketId;
    }

    public void setBucketId(UUID bucketId) {
        this.bucketId = bucketId;
    }

    public Long getDepositId() {
        return depositId;
    }

    public void setDepositId(Long depositId) {
        this.depositId = depositId;
    }

    public String getEndpoint() {
        return endpoint;
    }
}
