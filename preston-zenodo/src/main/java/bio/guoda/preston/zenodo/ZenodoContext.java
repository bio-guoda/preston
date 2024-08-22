package bio.guoda.preston.zenodo;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.rdf.api.IRI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ZenodoContext implements ZenodoConfig {

    private final String endpoint;
    private final List<String> communities;
    private Long depositId;
    private UUID bucketId;
    private final String accessToken;
    private JsonNode metadata;


    private boolean skipOnExisting = false;
    private final IRI provenanceAnchor;

    public ZenodoContext(IRI provenanceAnchor, String accessToken) {
        this(provenanceAnchor, accessToken, "https://sandbox.zenodo.org");
    }

    public ZenodoContext(IRI provenanceAnchor, String accessToken, String endpoint) {
        this(provenanceAnchor, accessToken, endpoint, Collections.emptyList());
    }

    public ZenodoContext(IRI provenanceAnchor, String accessToken, String endpoint, List<String> communities) {
        this.provenanceAnchor = provenanceAnchor;
        this.accessToken = accessToken;
        this.endpoint = endpoint;
        this.communities = communities;
    }

    public ZenodoContext(ZenodoConfig config) {
        this.provenanceAnchor = config.getProvenanceAnchor();
        this.accessToken = config.getAccessToken();
        this.endpoint = config.getEndpoint();
        this.communities = new ArrayList<>(config.getCommunities());
        this.skipOnExisting = config.shouldSkipOnExisting();
    }


    @Override
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

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public List<String> getCommunities() {
        return this.communities;
    }

    public void setMetadata(JsonNode metadata) {
        this.metadata = metadata;
    }

    public JsonNode getMetadata() {
        return metadata;
    }

    @Override
    public void setSkipOnExisting(Boolean skipOnExisting) {
        this.skipOnExisting = skipOnExisting;
    }

    @Override
    public boolean shouldSkipOnExisting() {
        return skipOnExisting;
    }

    @Override
    public IRI getProvenanceAnchor() {
        return provenanceAnchor;
    }


}
