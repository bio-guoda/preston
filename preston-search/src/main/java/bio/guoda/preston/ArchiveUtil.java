package bio.guoda.preston;

import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.KeyTo1LevelPath;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class ArchiveUtil {

    public static IRI copyDirectoryToBlobstoreAsTarGz(BlobStore blobStore, File directory) throws IOException {
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("archive", ".tmp");
            try (
                    OutputStream fileOut = FileUtils.openOutputStream(tmpFile);
                    OutputStream gzipOut = new GzipCompressorOutputStream(fileOut);
                    ArchiveOutputStream archiveOut = new TarArchiveOutputStream(gzipOut)) {

                ArchiveUtil.copyDirectoryToArchive(directory, archiveOut);

                archiveOut.finish();
            }

            return blobStore.put(FileUtils.openInputStream(tmpFile));

        } finally {
            FileUtils.deleteQuietly(tmpFile);
        }
    }

    public static void copyDirectoryToArchive(File directory, ArchiveOutputStream archiveOut) throws IOException {
        Path dirPath = directory.toPath();
        try (Stream<Path> walk = Files.walk(dirPath)) {
            walk.forEach(filePath -> {
                try {
                    if (!filePath.equals(dirPath)) {
                        File file = filePath.toFile();
                        Path destinationInArchive = dirPath.relativize(filePath);

                        ArchiveEntry entry = archiveOut.createArchiveEntry(file, destinationInArchive.toString());
                        archiveOut.putArchiveEntry(entry);

                        if (file.isFile()) {
                            IOUtils.copy(filePath.toUri().toURL(), archiveOut);
                        }

                        archiveOut.closeArchiveEntry();
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy file to archive", e);
                }
            });
        }
    }

    public static void unpackTarGzFromBlobstore(BlobStoreReadOnly blobStore, IRI version, File tmpDirectory) throws IOException {
        URI hash = new KeyTo1LevelPath(tmpDirectory.toURI(), HashKeyUtil.hashTypeFor(version)).toPath(version);
        File destination = new File(hash);

        try (InputStream gzipIn = new GzipCompressorInputStream(blobStore.get(version));
             ArchiveInputStream archiveIn = new TarArchiveInputStream(gzipIn)) {
            unpackArchive(destination, archiveIn);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void unpackArchive(File destination, ArchiveInputStream archiveIn) throws IOException {
        while (true) {
            ArchiveEntry entry = archiveIn.getNextEntry();
            if (entry == null) {
                break;
            }

            File entryDestination = new File(destination, entry.getName());
            if (entry.isDirectory()) {
                entryDestination.mkdir();
            } else {
                entryDestination.getParentFile().mkdirs();
                OutputStream out = Files.newOutputStream(entryDestination.toPath());
                IOUtils.copy(archiveIn, out);
            }
        }
    }

}
