package dev.wixforth.columnar.benchmark;

import dev.wixforth.columnar.baseline.RowBaselineExecutor;
import dev.wixforth.columnar.baseline.RowStoreTable;
import dev.wixforth.columnar.schema.ColumnDef;
import dev.wixforth.columnar.schema.ColumnType;
import dev.wixforth.columnar.schema.DateUtil;
import dev.wixforth.columnar.schema.Schema;
import dev.wixforth.columnar.storage.Database;
import dev.wixforth.columnar.storage.TableStorage;
import dev.wixforth.columnar.vector.ColumnVector;
import dev.wixforth.columnar.vector.DoubleColumnVector;
import dev.wixforth.columnar.vector.IntColumnVector;
import dev.wixforth.columnar.vector.LongColumnVector;
import dev.wixforth.columnar.vector.StringColumnVector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Deterministic TPC-H-compatible dataset generator for customer, orders, and lineitem tables.
 *
 * Writes identical datasets simultaneously into the micro-partitioned columnar Database and
 * (optionally) a row-oriented RowStoreTable baseline. Orders and lineitems are ingested in
 * realistic time-clustered arrival order with +/-15-day jitter, mirroring cloud warehouse ingestion.
 */
public final class TpchDataGenerator {
    public static final Schema CUSTOMER_SCHEMA = new Schema(List.of(
            new ColumnDef("c_custkey", ColumnType.INT64),
            new ColumnDef("c_mktsegment", ColumnType.STRING),
            new ColumnDef("c_nationkey", ColumnType.INT64),
            new ColumnDef("c_acctbal", ColumnType.DOUBLE)
    ));

    public static final Schema ORDERS_SCHEMA = new Schema(List.of(
            new ColumnDef("o_orderkey", ColumnType.INT64),
            new ColumnDef("o_custkey", ColumnType.INT64),
            new ColumnDef("o_orderstatus", ColumnType.STRING),
            new ColumnDef("o_totalprice", ColumnType.DOUBLE),
            new ColumnDef("o_orderdate", ColumnType.DATE),
            new ColumnDef("o_shippriority", ColumnType.INT64)
    ));

    public static final Schema LINEITEM_SCHEMA = new Schema(List.of(
            new ColumnDef("l_orderkey", ColumnType.INT64),
            new ColumnDef("l_partkey", ColumnType.INT64),
            new ColumnDef("l_linenumber", ColumnType.INT64),
            new ColumnDef("l_quantity", ColumnType.DOUBLE),
            new ColumnDef("l_extendedprice", ColumnType.DOUBLE),
            new ColumnDef("l_discount", ColumnType.DOUBLE),
            new ColumnDef("l_tax", ColumnType.DOUBLE),
            new ColumnDef("l_returnflag", ColumnType.STRING),
            new ColumnDef("l_linestatus", ColumnType.STRING),
            new ColumnDef("l_shipdate", ColumnType.DATE),
            new ColumnDef("l_shipmode", ColumnType.STRING)
    ));

    private static final String[] SEGMENTS = {"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"};
    private static final String[] ORDER_STATUSES = {"F", "O", "P"};
    private static final String[] RETURN_FLAGS = {"A", "N", "R"};
    private static final String[] LINE_STATUSES = {"F", "O"};
    private static final String[] SHIP_MODES = {"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"};

    private static final int START_DAY = DateUtil.parseEpochDay("1992-01-01");
    private static final int TOTAL_DAYS = 2500; // ~1992-01-01 to 1998-11-05
    private static final int CUTOFF_DAY = DateUtil.parseEpochDay("1995-06-17");

    private TpchDataGenerator() {}

    public record GeneratedStats(long customerRows, long orderRows, long lineitemRows, long genMillis) {}

    public static GeneratedStats generate(
            Database db,
            RowBaselineExecutor baseline,
            Path rowStoreDir,
            double scaleFactor,
            int partitionRows,
            long seed) throws IOException {
        long t0 = System.nanoTime();
        SplittableRandom rng = new SplittableRandom(seed);

        int numCustomers = Math.max(100, (int) Math.round(150_000 * scaleFactor));
        int numOrders = Math.max(400, (int) Math.round(1_500_000 * scaleFactor));

        // 1. CUSTOMER
        TableStorage custCol = db.createTable("customer", CUSTOMER_SCHEMA);
        RowStoreTable custRow = baseline != null
                ? new RowStoreTable("customer", CUSTOMER_SCHEMA, rowStoreDir.resolve("customer.row")) : null;
        if (baseline != null) baseline.registerTable(custRow);

        generateCustomers(custCol, custRow, numCustomers, partitionRows, rng);

        // 2. ORDERS + LINEITEM (correlated keys & realistic time-clustered arrival order)
        TableStorage ordCol = db.createTable("orders", ORDERS_SCHEMA);
        TableStorage liCol = db.createTable("lineitem", LINEITEM_SCHEMA);
        RowStoreTable ordRow = baseline != null
                ? new RowStoreTable("orders", ORDERS_SCHEMA, rowStoreDir.resolve("orders.row")) : null;
        RowStoreTable liRow = baseline != null
                ? new RowStoreTable("lineitem", LINEITEM_SCHEMA, rowStoreDir.resolve("lineitem.row")) : null;
        if (baseline != null) {
            baseline.registerTable(ordRow);
            baseline.registerTable(liRow);
        }

        long totalLineitems = generateOrdersAndLineitems(
                ordCol, ordRow, liCol, liRow, numCustomers, numOrders, partitionRows, rng);

        long millis = (System.nanoTime() - t0) / 1_000_000L;
        return new GeneratedStats(numCustomers, numOrders, totalLineitems, millis);
    }

    private static void generateCustomers(
            TableStorage colTable,
            RowStoreTable rowTable,
            int numCustomers,
            int partRows,
            SplittableRandom rng) throws IOException {
        try (RowStoreTable.Writer rw = rowTable != null ? rowTable.openWriter() : null) {
            long[] keys = new long[partRows];
            int[] segCodes = new int[partRows];
            long[] nations = new long[partRows];
            double[] acctbal = new double[partRows];
            Object[] rowBuf = new Object[4];
            int buffered = 0;

            for (int i = 1; i <= numCustomers; i++) {
                int seg = rng.nextInt(SEGMENTS.length);
                long nation = rng.nextInt(25);
                double bal = Math.round((rng.nextDouble(-999.99, 9999.99)) * 100.0) / 100.0;

                keys[buffered] = i;
                segCodes[buffered] = seg;
                nations[buffered] = nation;
                acctbal[buffered] = bal;
                buffered++;

                if (rw != null) {
                    rowBuf[0] = (long) i;
                    rowBuf[1] = SEGMENTS[seg];
                    rowBuf[2] = nation;
                    rowBuf[3] = bal;
                    rw.writeRow(rowBuf);
                }

                if (buffered == partRows) {
                    flushCustomers(colTable, keys, segCodes, nations, acctbal, buffered);
                    buffered = 0;
                }
            }
            if (buffered > 0) {
                flushCustomers(colTable, keys, segCodes, nations, acctbal, buffered);
            }
        }
    }

    private static void flushCustomers(
            TableStorage table, long[] keys, int[] segCodes, long[] nations, double[] acctbal, int n)
            throws IOException {
        ColumnVector[] vecs = new ColumnVector[]{
                new LongColumnVector(keys, n),
                new StringColumnVector(SEGMENTS, segCodes, n),
                new LongColumnVector(nations, n),
                new DoubleColumnVector(acctbal, n)
        };
        table.appendPartition(vecs, n);
    }

    private static long generateOrdersAndLineitems(
            TableStorage ordCol,
            RowStoreTable ordRow,
            TableStorage liCol,
            RowStoreTable liRow,
            int numCustomers,
            int numOrders,
            int partRows,
            SplittableRandom rng) throws IOException {

        long[] oKey = new long[partRows];
        long[] oCust = new long[partRows];
        int[] oStatus = new int[partRows];
        double[] oPrice = new double[partRows];
        int[] oDate = new int[partRows];
        long[] oPrio = new long[partRows];
        int ordBuf = 0;

        long[] lOrder = new long[partRows];
        long[] lPart = new long[partRows];
        long[] lLine = new long[partRows];
        double[] lQty = new double[partRows];
        double[] lPrice = new double[partRows];
        double[] lDisc = new double[partRows];
        double[] lTax = new double[partRows];
        int[] lRet = new int[partRows];
        int[] lStat = new int[partRows];
        int[] lShip = new int[partRows];
        int[] lMode = new int[partRows];
        int liBuf = 0;
        long totalLi = 0;

        Object[] ordRowObj = new Object[6];
        Object[] liRowObj = new Object[11];

        try (RowStoreTable.Writer oWriter = ordRow != null ? ordRow.openWriter() : null;
             RowStoreTable.Writer lWriter = liRow != null ? liRow.openWriter() : null) {

            for (int oi = 1; oi <= numOrders; oi++) {
                // Chronological arrival progress with natural +/- 10-day jitter
                int baseDay = START_DAY + (int) (((long) (oi - 1) * (TOTAL_DAYS - 130)) / Math.max(1, numOrders));
                int orderDate = Math.max(START_DAY, baseDay + rng.nextInt(-10, 11));
                long custKey = 1L + rng.nextInt(numCustomers);
                long shipPriority = rng.nextInt(3);

                // 1..7 lineitems per order (mean = 4 -> SF 1 => ~6M lineitems)
                int items = 1 + rng.nextInt(7);
                double orderTotal = 0.0;
                int openCount = 0;
                int fulfilledCount = 0;

                for (int ln = 1; ln <= items; ln++) {
                    long partKey = 1L + rng.nextInt(200_000);
                    double qty = 1 + rng.nextInt(50);
                    double unitPrice = 900.0 + (partKey % 1000);
                    double extPrice = Math.round(qty * unitPrice * 100.0) / 100.0;
                    double disc = rng.nextInt(11) / 100.0;
                    double tax = rng.nextInt(9) / 100.0;
                    int shipDate = orderDate + 1 + rng.nextInt(120);

                    int retIdx;
                    int statIdx;
                    if (shipDate <= CUTOFF_DAY) {
                        retIdx = rng.nextBoolean() ? 0 : 2; // A or R
                        statIdx = 0; // F
                        fulfilledCount++;
                    } else {
                        retIdx = 1; // N
                        statIdx = 1; // O
                        openCount++;
                    }
                    int modeIdx = rng.nextInt(SHIP_MODES.length);
                    orderTotal += extPrice * (1.0 - disc) * (1.0 + tax);

                    lOrder[liBuf] = oi;
                    lPart[liBuf] = partKey;
                    lLine[liBuf] = ln;
                    lQty[liBuf] = qty;
                    lPrice[liBuf] = extPrice;
                    lDisc[liBuf] = disc;
                    lTax[liBuf] = tax;
                    lRet[liBuf] = retIdx;
                    lStat[liBuf] = statIdx;
                    lShip[liBuf] = shipDate;
                    lMode[liBuf] = modeIdx;
                    liBuf++;
                    totalLi++;

                    if (lWriter != null) {
                        liRowObj[0] = (long) oi;
                        liRowObj[1] = partKey;
                        liRowObj[2] = (long) ln;
                        liRowObj[3] = qty;
                        liRowObj[4] = extPrice;
                        liRowObj[5] = disc;
                        liRowObj[6] = tax;
                        liRowObj[7] = RETURN_FLAGS[retIdx];
                        liRowObj[8] = LINE_STATUSES[statIdx];
                        liRowObj[9] = shipDate;
                        liRowObj[10] = SHIP_MODES[modeIdx];
                        lWriter.writeRow(liRowObj);
                    }

                    if (liBuf == partRows) {
                        flushLineitems(liCol, lOrder, lPart, lLine, lQty, lPrice, lDisc, lTax,
                                lRet, lStat, lShip, lMode, liBuf);
                        liBuf = 0;
                    }
                }

                int ordStatIdx = (openCount == 0) ? 0 : (fulfilledCount == 0 ? 1 : 2);
                double roundedTotal = Math.round(orderTotal * 100.0) / 100.0;

                oKey[ordBuf] = oi;
                oCust[ordBuf] = custKey;
                oStatus[ordBuf] = ordStatIdx;
                oPrice[ordBuf] = roundedTotal;
                oDate[ordBuf] = orderDate;
                oPrio[ordBuf] = shipPriority;
                ordBuf++;

                if (oWriter != null) {
                    ordRowObj[0] = (long) oi;
                    ordRowObj[1] = custKey;
                    ordRowObj[2] = ORDER_STATUSES[ordStatIdx];
                    ordRowObj[3] = roundedTotal;
                    ordRowObj[4] = orderDate;
                    ordRowObj[5] = shipPriority;
                    oWriter.writeRow(ordRowObj);
                }

                if (ordBuf == partRows) {
                    flushOrders(ordCol, oKey, oCust, oStatus, oPrice, oDate, oPrio, ordBuf);
                    ordBuf = 0;
                }
            }

            if (ordBuf > 0) {
                flushOrders(ordCol, oKey, oCust, oStatus, oPrice, oDate, oPrio, ordBuf);
            }
            if (liBuf > 0) {
                flushLineitems(liCol, lOrder, lPart, lLine, lQty, lPrice, lDisc, lTax,
                        lRet, lStat, lShip, lMode, liBuf);
            }
        }
        return totalLi;
    }

    private static void flushOrders(
            TableStorage table, long[] oKey, long[] oCust, int[] oStatus,
            double[] oPrice, int[] oDate, long[] oPrio, int n) throws IOException {
        ColumnVector[] vecs = new ColumnVector[]{
                new LongColumnVector(oKey, n),
                new LongColumnVector(oCust, n),
                new StringColumnVector(ORDER_STATUSES, oStatus, n),
                new DoubleColumnVector(oPrice, n),
                new IntColumnVector(oDate, n),
                new LongColumnVector(oPrio, n)
        };
        table.appendPartition(vecs, n);
    }

    private static void flushLineitems(
            TableStorage table, long[] lOrder, long[] lPart, long[] lLine, double[] lQty,
            double[] lPrice, double[] lDisc, double[] lTax, int[] lRet, int[] lStat,
            int[] lShip, int[] lMode, int n) throws IOException {
        ColumnVector[] vecs = new ColumnVector[]{
                new LongColumnVector(lOrder, n),
                new LongColumnVector(lPart, n),
                new LongColumnVector(lLine, n),
                new DoubleColumnVector(lQty, n),
                new DoubleColumnVector(lPrice, n),
                new DoubleColumnVector(lDisc, n),
                new DoubleColumnVector(lTax, n),
                new StringColumnVector(RETURN_FLAGS, lRet, n),
                new StringColumnVector(LINE_STATUSES, lStat, n),
                new IntColumnVector(lShip, n),
                new StringColumnVector(SHIP_MODES, lMode, n)
        };
        table.appendPartition(vecs, n);
    }
}
