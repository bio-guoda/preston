package bio.guoda.preston.store;

import org.apache.commons.collections4.CollectionUtils;

public class KeyValueStoreFactoryImpl implements KeyValueStoreFactory {

    private final KeyValueStoreConfig config;

    public KeyValueStoreFactoryImpl(KeyValueStoreConfig config) {
        this.config = config;
    }

    @Override
    public KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory kvStreamFactory) {

        KeyValueStore keyValueStore
                = new KeyValueStoreFactoryFallBack(config).getKeyValueStore(kvStreamFactory);

        return CollectionUtils.isEmpty(config.getRemotes())
                ? keyValueStore
                : KeyValueStoreUtil.withRemoteSupport(kvStreamFactory, keyValueStore, config);
    }
}
