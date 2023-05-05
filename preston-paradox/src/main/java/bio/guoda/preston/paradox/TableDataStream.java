/*
 * Copyright (C) 2009 Leonardo Alves da Costa
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public
 * License for more details. You should have received a copy of the GNU General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 */
package bio.guoda.preston.paradox;

import bio.guoda.preston.process.ProcessorState;
import com.googlecode.paradox.data.ParadoxData;
import com.googlecode.paradox.data.ParadoxFieldFactory;
import com.googlecode.paradox.metadata.Field;
import com.googlecode.paradox.metadata.paradox.ParadoxTable;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Utility class for loading table files.
 *
 * @version 1.10
 * @since 1.0
 */
public final class TableDataStream extends ParadoxData {

    /**
     * Load the table data from file.
     *
     * @param state
     * @param data   the data to read.
     * @param fields the fields to read.
     * @return the row values.
     * @throws SQLException in case of failures.
     */
    public static void streamData(final ParadoxTable data,
                                  final Field[] fields,
                                  Consumer<Pair<Long, List<Pair<Field, Object>>>> sink,
                                  ProcessorState state
    ) throws IOException {

        AtomicLong rowNumberWithOffsetOne = new AtomicLong(1);

        final int blockSize = data.getBlockSizeBytes();
        final int recordSize = data.getRecordSize();
        final int headerSize = data.getHeaderSize();

        if (blockSize > 0 && recordSize > 0 && headerSize > 0) {

            try (final FileInputStream fs = new FileInputStream(data.getFile());
                 final FileChannel channel = fs.getChannel()) {


                long nextBlock = data.getFirstBlock();
                if (nextBlock != 0) {
                    final ByteBuffer buffer = ByteBuffer.allocate(blockSize);
                    do {
                        buffer.order(ByteOrder.LITTLE_ENDIAN);
                        long position = headerSize + ((nextBlock - 1) * blockSize);
                        channel.position(position);

                        buffer.clear();
                        channel.read(buffer);
                        checkDBEncryption(buffer, data, blockSize, nextBlock);
                        buffer.flip();

                        nextBlock = buffer.getShort() & 0xFFFF;

                        // The block number.
                        buffer.getShort();

                        final int addDataSize = buffer.getShort();
                        final int rowsInBlock = (addDataSize / recordSize) + 1;

                        buffer.order(ByteOrder.BIG_ENDIAN);

                        for (int loop = 0; loop < rowsInBlock; loop++) {
                            sink.accept(Pair.of(rowNumberWithOffsetOne.getAndIncrement(), readRow(data, fields, buffer)));
                        }
                    } while (nextBlock != 0 && state.shouldKeepProcessing());
                }

            } catch (final IOException e) {
                throw new IOException("failed to load [" + data.getName() + "]", e);
            }
        }
    }

    private static void streamTableData(ParadoxTable data, Field[] fields, Consumer<Pair<Long, List<Pair<Field, Object>>>> sink) throws IOException {
        AtomicLong rowNumberWithOffsetOne = new AtomicLong(1);

        final int blockSize = data.getBlockSizeBytes();
        final int recordSize = data.getRecordSize();
        final int headerSize = data.getHeaderSize();

        if (blockSize > 0 && recordSize > 0 && headerSize > 0) {

            try (final FileInputStream fs = new FileInputStream(data.getFile());
                 final FileChannel channel = fs.getChannel()) {

                long nextBlock = data.getFirstBlock();

                final ByteBuffer buffer = ByteBuffer.allocate(blockSize);
                do {
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    long position = headerSize + ((nextBlock - 1) * blockSize);
                    channel.position(position);

                    buffer.clear();
                    channel.read(buffer);
                    checkDBEncryption(buffer, data, blockSize, nextBlock);
                    buffer.flip();

                    nextBlock = buffer.getShort() & 0xFFFF;

                    // The block number.
                    buffer.getShort();

                    final int addDataSize = buffer.getShort();
                    final int rowsInBlock = (addDataSize / recordSize) + 1;

                    buffer.order(ByteOrder.BIG_ENDIAN);

                    for (int loop = 0; loop < rowsInBlock; loop++) {
                        sink.accept(Pair.of(rowNumberWithOffsetOne.getAndIncrement(), readRow(data, fields, buffer)));
                    }
                } while (nextBlock != 0);

            } catch (final IOException e) {
                throw new IOException("failed to load [" + data.getName() + "]");
            }
        }
    }

    /**
     * Read a entire row.
     *
     * @param table  the table to read of.
     * @param fields the fields to read.
     * @param buffer the buffer to read of.
     * @return the row.
     * @throws SQLException in case of parse errors.
     */
    private static List<Pair<Field, Object>> readRow(final ParadoxTable table,
                                                     final Field[] fields,
                                                     final ByteBuffer buffer) throws IOException {
        final List<Pair<Field, Object>> row = new ArrayList<>(fields.length);

        for (final Field field : table.getFields()) {
            if (Arrays.asList(Types.BLOB, Types.CLOB).contains(field.getSqlType())) {
                row.add(Pair.of(field, "BLOB_CLOB_TYPES_NOT_SUPPORTED"));
            } else {
                // Field filter
                final int index = search(fields, field);
                Object value = "";
                if (index != -1) {
                    try {
                        value = ParadoxFieldFactory.parse(table, buffer, field);
                    } catch (SQLException e) {
                        throw new IOException("failed to parse field [" + field.getName() + "] in table [" + table.getName() + "]", e);
                    }
                    row.add(Pair.of(field, value));
                } else {
                    int size = field.getRealSize();
                    buffer.position(buffer.position() + size);
                }
            }
        }

        if (row.size() != fields.length) {
            throw new IOException("row data misaligned with available table columns");
        }
        return row;
    }

    private static int search(final Field[] values, Object find) {
        for (int i = 0; i < values.length; i++) {
            if (Objects.equals(values[i], find)) {
                return i;
            }
        }

        return -1;
    }
}
