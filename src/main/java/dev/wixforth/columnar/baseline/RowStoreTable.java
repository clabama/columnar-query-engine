package dev.wixforth.columnar.baseline;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.Schema;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Simple binary row-oriented table stored contiguously on disk (one full tuple after another).
 * Serves as the fair single-threaded Volcano-iterator baseline for benchmarking against the
 * micro-partitioned columnar engine.
 */
public final class RowStoreTable {
    private final String name;
    private final Schema schema;
    private final Path dataFile;
    private long rowCount;
    private long fileBytes;

    public RowStoreTable(String name, Schema schema, Path dataFile) throws IOException {
        this.name = name;
        this.schema = schema;
        this.dataFile = dataFile;
        Files.createDirectories(dataFile.getParent());
    }

    public String name() {
        return name;
    }

    public Schema schema() {
        return schema;
    }

    public long rowCount() {
        return rowCount;
    }

    public long fileBytes() {
        return fileBytes;
    }

    public Writer openWriter() throws IOException {
        return new Writer();
    }

    public RowCursor openCursor() throws IOException {
        return new RowCursor();
    }

    public final class Writer implements AutoCloseable {
        private final DataOutputStream out;
        private long written = 0;

        Writer() throws IOException {
            this.out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(dataFile), 1 << 16));
        }

        public void writeRow(Object[] values) throws IOException {
            int cols = schema.columnCount();
            for (int c = 0; c < cols; c++) {
                switch (schema.column(c).type()) {
                    case INT64 -> out.writeLong((Long) values[c]);
                    case DOUBLE -> out.writeDouble((Double) values[c]);
                    case DATE -> out.writeInt((Integer) values[c]);
                    case STRING -> out.writeUTF((String) values[c]);
                }
            }
            written++;
        }

        @Override
        public void close() throws IOException {
            out.flush();
            out.close();
            rowCount = written;
            fileBytes = Files.size(dataFile);
        }
    }

    public final class RowCursor implements Iterator<Object[]>, AutoCloseable {
        private final DataInputStream in;
        private long remaining;
        private final ColumnType[] types;
        private final Object[] reusableRow;

        RowCursor() throws IOException {
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(dataFile), 1 << 16));
            this.remaining = rowCount;
            this.types = new ColumnType[schema.columnCount()];
            for (int i = 0; i < types.length; i++) {
                types[i] = schema.column(i).type();
            }
            this.reusableRow = new Object[types.length];
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public Object[] next() {
            if (remaining <= 0) throw new NoSuchElementException();
            try {
                for (int c = 0; c < types.length; c++) {
                    reusableRow[c] = switch (types[c]) {
                        case INT64 -> in.readLong();
                        case DOUBLE -> in.readDouble();
                        case DATE -> in.readInt();
                        case STRING -> in.readUTF();
                    };
                }
                remaining--;
                return reusableRow;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
