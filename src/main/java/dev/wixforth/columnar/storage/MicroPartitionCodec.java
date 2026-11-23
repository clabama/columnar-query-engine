package dev.wixforth.columnar.storage;

import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.Schema;
import dev.wixforth.columnar.vector.ColumnVector;
import dev.wixforth.columnar.vector.DoubleColumnVector;
import dev.wixforth.columnar.vector.IntColumnVector;
import dev.wixforth.columnar.vector.LongColumnVector;
import dev.wixforth.columnar.vector.StringColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary encoder/decoder for immutable columnar micro-partition (.mpart) files.
 *
 * File layout (little-endian):
 *   Header:  [4B magic 0x4D505254][1B version=1][4B rowCount][4B colCount]
 *   Column0: [encoded bytes]
 *   Column1: [encoded bytes]
 *   ...
 *
 * Per-column byte offsets and min/max zone maps are returned in PartitionMetadata so queries
 * can skip pruned partitions completely and read only projected columns via positional FileChannel I/O.
 */
public final class MicroPartitionCodec {
    public static final int MAGIC = 0x4D505254; // "MPRT"
    public static final byte VERSION = 1;
    private static final int HEADER_BYTES = 13;

    private MicroPartitionCodec() {}

    public static PartitionMetadata writePartition(
            int partitionId,
            Path filePath,
            Schema schema,
            ColumnVector[] columns,
            int rowCount) throws IOException {
        int colCount = schema.columnCount();
        byte[][] encodedCols = new byte[colCount][];
        EncodingType[] encodings = new EncodingType[colCount];
        ColumnStats[] stats = new ColumnStats[colCount];

        long currentOffset = HEADER_BYTES;
        for (int c = 0; c < colCount; c++) {
            ColumnType type = schema.column(c).type();
            switch (type) {
                case INT64 -> {
                    LongColumnVector vec = (LongColumnVector) columns[c];
                    long min = Long.MAX_VALUE;
                    long max = Long.MIN_VALUE;
                    for (int i = 0; i < rowCount; i++) {
                        long v = vec.get(i);
                        if (v < min) min = v;
                        if (v > max) max = v;
                    }
                    EncodedChunk chunk = encodeLongs(vec, rowCount);
                    encodedCols[c] = chunk.bytes;
                    encodings[c] = chunk.encoding;
                    stats[c] = ColumnStats.ofLong(chunk.encoding, currentOffset, chunk.bytes.length, min, max);
                    currentOffset += chunk.bytes.length;
                }
                case DOUBLE -> {
                    DoubleColumnVector vec = (DoubleColumnVector) columns[c];
                    double min = Double.POSITIVE_INFINITY;
                    double max = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < rowCount; i++) {
                        double v = vec.get(i);
                        if (v < min) min = v;
                        if (v > max) max = v;
                    }
                    EncodedChunk chunk = encodeDoubles(vec, rowCount);
                    encodedCols[c] = chunk.bytes;
                    encodings[c] = chunk.encoding;
                    stats[c] = ColumnStats.ofDouble(chunk.encoding, currentOffset, chunk.bytes.length, min, max);
                    currentOffset += chunk.bytes.length;
                }
                case DATE -> {
                    IntColumnVector vec = (IntColumnVector) columns[c];
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;
                    for (int i = 0; i < rowCount; i++) {
                        int v = vec.get(i);
                        if (v < min) min = v;
                        if (v > max) max = v;
                    }
                    EncodedChunk chunk = encodeInts(vec, rowCount);
                    encodedCols[c] = chunk.bytes;
                    encodings[c] = chunk.encoding;
                    stats[c] = ColumnStats.ofDate(chunk.encoding, currentOffset, chunk.bytes.length, min, max);
                    currentOffset += chunk.bytes.length;
                }
                case STRING -> {
                    StringColumnVector vec = (StringColumnVector) columns[c];
                    String min = null;
                    String max = null;
                    for (int i = 0; i < rowCount; i++) {
                        String s = vec.get(i);
                        if (min == null || s.compareTo(min) < 0) min = s;
                        if (max == null || s.compareTo(max) > 0) max = s;
                    }
                    EncodedChunk chunk = encodeStrings(vec, rowCount);
                    encodedCols[c] = chunk.bytes;
                    encodings[c] = chunk.encoding;
                    stats[c] = ColumnStats.ofString(chunk.encoding, currentOffset, chunk.bytes.length, min, max);
                    currentOffset += chunk.bytes.length;
                }
            }
        }

        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(MAGIC);
        header.put(VERSION);
        header.putInt(rowCount);
        header.putInt(colCount);
        header.flip();

        try (FileChannel ch = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            writeFully(ch, header);
            for (int c = 0; c < colCount; c++) {
                writeFully(ch, ByteBuffer.wrap(encodedCols[c]));
            }
        }

        return new PartitionMetadata(partitionId, filePath, rowCount, currentOffset, stats);
    }

    /**
     * Reads and decodes a single column from an immutable micro-partition file using positional I/O.
     */
    public static ColumnVector readColumn(PartitionMetadata meta, int colIdx) throws IOException {
        ColumnStats st = meta.stats(colIdx);
        ByteBuffer buf = ByteBuffer.allocate(st.byteLength).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel ch = FileChannel.open(meta.filePath, StandardOpenOption.READ)) {
            readFully(ch, buf, st.byteOffset);
        }
        buf.flip();

        int rowCount = meta.rowCount;
        return switch (st.type) {
            case INT64 -> decodeLongs(buf, st.encoding, rowCount);
            case DOUBLE -> decodeDoubles(buf, st.encoding, rowCount);
            case DATE -> decodeInts(buf, st.encoding, rowCount);
            case STRING -> decodeStrings(buf, st.encoding, rowCount);
        };
    }

    private record EncodedChunk(EncodingType encoding, byte[] bytes) {}

    private static EncodedChunk encodeLongs(LongColumnVector vec, int n) {
        int runs = countLongRuns(vec, n);
        int plainBytes = n * Long.BYTES;
        int rleBytes = 4 + runs * (Long.BYTES + Integer.BYTES);
        if (rleBytes < plainBytes * 0.85) {
            ByteBuffer buf = ByteBuffer.allocate(rleBytes).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(runs);
            int i = 0;
            while (i < n) {
                long val = vec.get(i);
                int runLen = 1;
                while (i + runLen < n && vec.get(i + runLen) == val) {
                    runLen++;
                }
                buf.putLong(val);
                buf.putInt(runLen);
                i += runLen;
            }
            return new EncodedChunk(EncodingType.RLE, buf.array());
        } else {
            ByteBuffer buf = ByteBuffer.allocate(plainBytes).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < n; i++) {
                buf.putLong(vec.get(i));
            }
            return new EncodedChunk(EncodingType.PLAIN, buf.array());
        }
    }

    private static LongColumnVector decodeLongs(ByteBuffer buf, EncodingType enc, int n) {
        long[] out = new long[n];
        if (enc == EncodingType.PLAIN) {
            for (int i = 0; i < n; i++) {
                out[i] = buf.getLong();
            }
        } else if (enc == EncodingType.RLE) {
            int runs = buf.getInt();
            int pos = 0;
            for (int r = 0; r < runs; r++) {
                long val = buf.getLong();
                int len = buf.getInt();
                for (int k = 0; k < len; k++) {
                    out[pos++] = val;
                }
            }
        } else {
            throw new IllegalStateException("Unsupported INT64 encoding: " + enc);
        }
        return new LongColumnVector(out, n);
    }

    private static EncodedChunk encodeDoubles(DoubleColumnVector vec, int n) {
        int runs = countDoubleRuns(vec, n);
        int plainBytes = n * Double.BYTES;
        int rleBytes = 4 + runs * (Double.BYTES + Integer.BYTES);
        if (rleBytes < plainBytes * 0.85) {
            ByteBuffer buf = ByteBuffer.allocate(rleBytes).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(runs);
            int i = 0;
            while (i < n) {
                double val = vec.get(i);
                long bits = Double.doubleToRawLongBits(val);
                int runLen = 1;
                while (i + runLen < n && Double.doubleToRawLongBits(vec.get(i + runLen)) == bits) {
                    runLen++;
                }
                buf.putDouble(val);
                buf.putInt(runLen);
                i += runLen;
            }
            return new EncodedChunk(EncodingType.RLE, buf.array());
        } else {
            ByteBuffer buf = ByteBuffer.allocate(plainBytes).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < n; i++) {
                buf.putDouble(vec.get(i));
            }
            return new EncodedChunk(EncodingType.PLAIN, buf.array());
        }
    }

    private static DoubleColumnVector decodeDoubles(ByteBuffer buf, EncodingType enc, int n) {
        double[] out = new double[n];
        if (enc == EncodingType.PLAIN) {
            for (int i = 0; i < n; i++) {
                out[i] = buf.getDouble();
            }
        } else if (enc == EncodingType.RLE) {
            int runs = buf.getInt();
            int pos = 0;
            for (int r = 0; r < runs; r++) {
                double val = buf.getDouble();
                int len = buf.getInt();
                for (int k = 0; k < len; k++) {
                    out[pos++] = val;
                }
            }
        } else {
            throw new IllegalStateException("Unsupported DOUBLE encoding: " + enc);
        }
        return new DoubleColumnVector(out, n);
    }

    private static EncodedChunk encodeInts(IntColumnVector vec, int n) {
        int runs = countIntRuns(vec, n);
        int plainBytes = n * Integer.BYTES;
        int rleBytes = 4 + runs * (Integer.BYTES + Integer.BYTES);
        if (rleBytes < plainBytes * 0.85) {
            ByteBuffer buf = ByteBuffer.allocate(rleBytes).order(ByteOrder.LITTLE_ENDIAN);
            buf.putInt(runs);
            int i = 0;
            while (i < n) {
                int val = vec.get(i);
                int runLen = 1;
                while (i + runLen < n && vec.get(i + runLen) == val) {
                    runLen++;
                }
                buf.putInt(val);
                buf.putInt(runLen);
                i += runLen;
            }
            return new EncodedChunk(EncodingType.RLE, buf.array());
        } else {
            ByteBuffer buf = ByteBuffer.allocate(plainBytes).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < n; i++) {
                buf.putInt(vec.get(i));
            }
            return new EncodedChunk(EncodingType.PLAIN, buf.array());
        }
    }

    private static IntColumnVector decodeInts(ByteBuffer buf, EncodingType enc, int n) {
        int[] out = new int[n];
        if (enc == EncodingType.PLAIN) {
            for (int i = 0; i < n; i++) {
                out[i] = buf.getInt();
            }
        } else if (enc == EncodingType.RLE) {
            int runs = buf.getInt();
            int pos = 0;
            for (int r = 0; r < runs; r++) {
                int val = buf.getInt();
                int len = buf.getInt();
                for (int k = 0; k < len; k++) {
                    out[pos++] = val;
                }
            }
        } else {
            throw new IllegalStateException("Unsupported DATE encoding: " + enc);
        }
        return new IntColumnVector(out, n);
    }

    private static EncodedChunk encodeStrings(StringColumnVector vec, int n) {
        // Re-compact dictionary for this partition
        Map<String, Integer> dictMap = new HashMap<>();
        List<byte[]> dictBytes = new ArrayList<>();
        int[] codes = new int[n];
        int totalDictPayload = 4; // dictSize int
        for (int i = 0; i < n; i++) {
            String s = vec.get(i);
            Integer id = dictMap.get(s);
            if (id == null) {
                id = dictBytes.size();
                dictMap.put(s, id);
                byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
                dictBytes.add(utf8);
                totalDictPayload += 4 + utf8.length;
            }
            codes[i] = id;
        }

        int runs = countIntArrayRuns(codes, n);
        int plainCodeBytes = n * Integer.BYTES;
        int rleCodeBytes = 4 + runs * 8;
        boolean useRle = rleCodeBytes < plainCodeBytes * 0.85;
        int totalBytes = totalDictPayload + (useRle ? rleCodeBytes : plainCodeBytes);

        ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(dictBytes.size());
        for (byte[] utf8 : dictBytes) {
            buf.putInt(utf8.length);
            buf.put(utf8);
        }
        if (useRle) {
            buf.putInt(runs);
            int i = 0;
            while (i < n) {
                int code = codes[i];
                int len = 1;
                while (i + len < n && codes[i + len] == code) {
                    len++;
                }
                buf.putInt(code);
                buf.putInt(len);
                i += len;
            }
            return new EncodedChunk(EncodingType.DICTIONARY_RLE, buf.array());
        } else {
            for (int i = 0; i < n; i++) {
                buf.putInt(codes[i]);
            }
            return new EncodedChunk(EncodingType.DICTIONARY, buf.array());
        }
    }

    private static StringColumnVector decodeStrings(ByteBuffer buf, EncodingType enc, int n) {
        int dictSize = buf.getInt();
        String[] dict = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            int len = buf.getInt();
            byte[] utf8 = new byte[len];
            buf.get(utf8);
            dict[i] = new String(utf8, StandardCharsets.UTF_8);
        }
        int[] codes = new int[n];
        if (enc == EncodingType.DICTIONARY) {
            for (int i = 0; i < n; i++) {
                codes[i] = buf.getInt();
            }
        } else if (enc == EncodingType.DICTIONARY_RLE) {
            int runs = buf.getInt();
            int pos = 0;
            for (int r = 0; r < runs; r++) {
                int code = buf.getInt();
                int len = buf.getInt();
                for (int k = 0; k < len; k++) {
                    codes[pos++] = code;
                }
            }
        } else {
            throw new IllegalStateException("Unsupported STRING encoding: " + enc);
        }
        return new StringColumnVector(dict, codes, n);
    }

    private static int countLongRuns(LongColumnVector vec, int n) {
        if (n == 0) return 0;
        int runs = 1;
        long prev = vec.get(0);
        for (int i = 1; i < n; i++) {
            long cur = vec.get(i);
            if (cur != prev) {
                runs++;
                prev = cur;
            }
        }
        return runs;
    }

    private static int countDoubleRuns(DoubleColumnVector vec, int n) {
        if (n == 0) return 0;
        int runs = 1;
        long prev = Double.doubleToRawLongBits(vec.get(0));
        for (int i = 1; i < n; i++) {
            long cur = Double.doubleToRawLongBits(vec.get(i));
            if (cur != prev) {
                runs++;
                prev = cur;
            }
        }
        return runs;
    }

    private static int countIntRuns(IntColumnVector vec, int n) {
        if (n == 0) return 0;
        int runs = 1;
        int prev = vec.get(0);
        for (int i = 1; i < n; i++) {
            int cur = vec.get(i);
            if (cur != prev) {
                runs++;
                prev = cur;
            }
        }
        return runs;
    }

    private static int countIntArrayRuns(int[] arr, int n) {
        if (n == 0) return 0;
        int runs = 1;
        int prev = arr[0];
        for (int i = 1; i < n; i++) {
            if (arr[i] != prev) {
                runs++;
                prev = arr[i];
            }
        }
        return runs;
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int read = ch.read(buf, pos);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading micro-partition at offset " + pos);
            }
            pos += read;
        }
    }
}
