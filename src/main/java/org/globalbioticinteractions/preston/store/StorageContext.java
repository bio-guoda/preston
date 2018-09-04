package org.globalbioticinteractions.preston.store;

public interface StorageContext {

    BlobStore getBlobStore();

    StatementStore getStatementStore();

}
