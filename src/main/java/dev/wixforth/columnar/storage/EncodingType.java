package dev.wixforth.columnar.storage;

/**
 * Per-column physical compression encoding stored inside a micro-partition file.
 */
public enum EncodingType {
    PLAIN((byte) 0),
    RLE((byte) 1),
    DICTIONARY((byte) 2),
    DICTIONARY_RLE((byte) 3);

    public final byte code;

    EncodingType(byte code) {
        this.code = code;
    }

    public static EncodingType fromCode(byte code) {
        for (EncodingType e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        throw new IllegalArgumentException("Unknown encoding code: " + code);
    }
}
