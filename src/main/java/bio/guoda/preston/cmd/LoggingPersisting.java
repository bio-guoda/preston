package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreCopying;
import bio.guoda.preston.store.KeyValueStoreRemoteHTTP;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.URLConverter;

import java.net.URL;

public class LoggingPersisting extends Persisting {

    @Parameter(names = {"-l", "--log",}, description = "log format", converter = LoggerConverter.class)
    private Logger logMode = Logger.nquads;

    protected Logger getLogMode() {
        return logMode;
    }

    @Parameter(names = {"--remote"}, description = "remote url", converter = URLConverter.class)
    private URL remoteURL = null;

    protected URL getRemoteURL() {
        return remoteURL;
    }

    protected boolean hasRemote() {
        return remoteURL != null;
    }

    @Override
    KeyValueStore getKeyValueStore() {
        KeyValueStore store;
        if (hasRemote()) {
            store = new KeyValueStoreCopying(new KeyValueStoreRemoteHTTP(getRemoteURL()), super.getKeyValueStore());
        } else {
            store = super.getKeyValueStore();
        }
        return store;
    }
}
