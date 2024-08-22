package bio.guoda.preston.zenodo;

import org.apache.commons.rdf.api.IRI;

import java.util.List;

public interface ZenodoConfig {
    String getAccessToken();

    String getEndpoint();

    List<String> getCommunities();

    void setCreateNewVersionForExisting(Boolean skipOnExisting);

    boolean createNewVersionForExisting();
}
