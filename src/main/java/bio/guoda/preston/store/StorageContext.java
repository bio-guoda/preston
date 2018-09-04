package bio.guoda.preston.store;

public interface StorageContext {

    BlobStore getBlobStore();

    StatementStore getStatementStore();

}
