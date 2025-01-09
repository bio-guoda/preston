package bio.guoda.preston.store;

public interface KeyValueStoreFactory {
    KeyValueStore getKeyValueStore(ValidatingKeyValueStreamFactory validatingKeyValueStreamFactory);
}
