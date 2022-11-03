package bio.guoda.preston;

import bio.guoda.preston.store.BlobStore;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class ArchiveUtil {

    public static IRI copyDirectoryToBlobstore(BlobStore blobStore, File directory) throws IOException {
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
                    File file = filePath.toFile();
                    ArchiveEntry entry = archiveOut.createArchiveEntry(file, filePath.relativize(dirPath).toString());
                    archiveOut.putArchiveEntry(entry);
                    if (file.isFile()) {
                        IOUtils.copy(filePath.toUri().toURL(), archiveOut);
                    }
                    archiveOut.closeArchiveEntry();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy file to archive", e);
                }
            });
        }
    }

}
