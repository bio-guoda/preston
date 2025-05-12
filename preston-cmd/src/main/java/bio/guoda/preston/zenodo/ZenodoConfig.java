package bio.guoda.preston.zenodo;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.util.List;

public interface ZenodoConfig {
    String getAccessToken();

    String getEndpoint();

    List<String> getCommunities();

    List<Pair<String, IRI>> getFileVersions();

    void setCreateNewVersionForExisting(Boolean skipOnExisting);

    boolean createNewVersionForExisting();

    void setPublishRestrictedOnly(boolean restrictedOnly);

    boolean shouldPublishRestrictedOnly();

    void setUpdateMetadataOnly(boolean updateMetadataOnly);

    boolean shouldUpdateMetadataOnly();

    void setAllowEmptyPublicationDate(boolean allowEmptyPublicationDate);

    boolean shouldAllowEmptyPublicationDate();

    IRI getLicenseRelations();

    String getTmpDir();
}
