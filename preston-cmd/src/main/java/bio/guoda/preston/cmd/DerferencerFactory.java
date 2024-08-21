package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;

import java.io.InputStream;

public interface DerferencerFactory {

    Dereferencer<InputStream> create();
}
