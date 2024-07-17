package bio.guoda.preston.zenodo;

import java.util.List;

public interface ZenodoConfig {
    String getAccessToken();

    String getEndpoint();

    List<String> getCommunities();
}
