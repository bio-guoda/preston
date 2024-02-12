package bio.guoda.preston.cmd;

import java.util.UUID;

public class ZenodoContext {

    private Long depositId;
    private UUID bucketId;
    private String accessToken;

    public ZenodoContext(String accessToken) {
        this.accessToken = accessToken;
    }


    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
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
}
