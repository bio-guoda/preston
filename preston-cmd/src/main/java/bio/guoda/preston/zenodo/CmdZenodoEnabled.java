package bio.guoda.preston.zenodo;

import bio.guoda.preston.EnvUtil;
import bio.guoda.preston.cmd.LoggingPersisting;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import java.util.Collections;
import java.util.List;

public abstract class CmdZenodoEnabled extends LoggingPersisting implements Runnable {

    public static final String ZENODO_ENDPOINT = "ZENODO_ENDPOINT";
    public static final String ZENODO_TOKEN = "ZENODO_TOKEN";

    private String accessToken = EnvUtil.getEnvironmentVariable(ZENODO_TOKEN);

    @CommandLine.Option(
            names = {"--endpoint"},
            description = "Zenodo api endpoint. Uses [ " + ZENODO_ENDPOINT + "] environment variable by default."
    )
    private String apiEndpoint = EnvUtil.getEnvironmentVariable(ZENODO_ENDPOINT, "https://zenodo.org");

    public void setCreateNewVersionForExisting(Boolean createNewVersionForExisting) {
        this.createNewVersionForExisting = createNewVersionForExisting;
    }

    @CommandLine.Option(
            names = {"--new-version"},
            description = "deposit new version if a Zenodo deposit with matching identifiers already exists"
    )
    private Boolean createNewVersionForExisting = false;

    @CommandLine.Option(
            names = {"--communities", "--community"},
            description = "associated Zenodo communities"
    )
    private List<String> communities = Collections.emptyList();

    @CommandLine.Option(
            names = {"--licenses", "--license"},
            description = "contentid (e.g., sha256, md5) to resource that associates licenses for (alternate) identifiers: when provided, only deposits with identified licenses are published. The license relations are expected in rdf/nquads format:" +
                    " <some:id> <http://purl.org/dc/elements/1.1/license> <https://spdx.org/licenses/...> .\n" +
                    " For example:\n" +
                    " <urn:lsid:biodiversitylibrary.org:part:94849> <http://purl.org/dc/elements/1.1/license> <https://spdx.org/licenses/CC-BY-NC-SA-3.0> ."
    )
    private IRI licenseRelations = null;

    @CommandLine.Option(
            names = {"--restricted-access-only"},
            description = "always set [access_right] to [restricted]"
    )
    private boolean publishRestrictedOnly = false;

    @CommandLine.Option(
            names = {"--explicit-license-only"},
            description = "only deposit records with explicit licenses: default license not allowed."
    )
    private boolean explicitLicenseOnly = false;

    @CommandLine.Option(
            names = {"--update-metadata-only"},
            description = "update metadata of existing record(s) only: if no associated record exists do nothing."
    )
    private boolean updateMetadataOnly;

    @CommandLine.Option(
            names = {"--allow-empty-publication-date"},
            description = "Zenodo accepts deposits with empty publication dates. On accepting a deposit without publication date, Zenodo sets the publication date to current date/time by default."
    )
    private boolean allowEmptyPublicationDate;

    protected ZenodoConfig getZenodoContext() {
        ZenodoContext zenodoContext = new ZenodoContext(
                getAccessToken(),
                getApiEndpoint(),
                getCommunities(),
                getLicenseRelations()
        );
        zenodoContext.setCreateNewVersionForExisting(createNewVersionForExisting);
        zenodoContext.setPublishRestrictedOnly(publishRestrictedOnly);
        zenodoContext.setExplicitLicenseOnly(explicitLicenseOnly);
        zenodoContext.setUpdateMetadataOnly(updateMetadataOnly);
        zenodoContext.setAllowEmptyPublicationDate(allowEmptyPublicationDate);
        zenodoContext.setTmpDir(getTmpDir());
        return zenodoContext;
    }

    private IRI getLicenseRelations() {
        return licenseRelations;
    }

    public void setLicenseRelations(IRI licenseRelations) {
        this.licenseRelations = licenseRelations;
    }

    private String getAccessToken() {
        return accessToken;
    }

    public String getApiEndpoint() {
        return apiEndpoint;
    }

    public void setApiEndpoint(String apiEndpoint) {
        this.apiEndpoint = apiEndpoint;
    }

    public List<String> getCommunities() {
        return communities;
    }

    public void setPublishRestrictedOnly(boolean publishedRestrictedOnly) {
        this.publishRestrictedOnly = publishedRestrictedOnly;
    }

    public boolean shouldPublishRestrictedOnly() {
        return publishRestrictedOnly;
    }

    public void setUpdateMetadataOnly(boolean updateMetadataOnly) {
        this.updateMetadataOnly = updateMetadataOnly;
    }

    public boolean shouldUpdateMetadataOnly() {
        return updateMetadataOnly;
    }

    public void setExplicitLicenseOnly(boolean explicitLicenseOnly) {
        this.explicitLicenseOnly = explicitLicenseOnly;
    }

    public boolean shouldAllowExplicitLicenseOnly() {
        return updateMetadataOnly;
    }
}
