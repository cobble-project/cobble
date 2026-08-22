package io.cobble.table;

/** Factories for logical types; unparameterized values are cached by nullability. */
public final class LogicalTypes {
    private LogicalTypes() {}

    private static final BooleanType BOOLEAN = new BooleanType(false),
            N_BOOLEAN = new BooleanType(true);
    private static final Int8Type INT8 = new Int8Type(false), N_INT8 = new Int8Type(true);
    private static final Int16Type INT16 = new Int16Type(false), N_INT16 = new Int16Type(true);
    private static final Int32Type INT32 = new Int32Type(false), N_INT32 = new Int32Type(true);
    private static final Int64Type INT64 = new Int64Type(false), N_INT64 = new Int64Type(true);
    private static final Float32Type FLOAT32 = new Float32Type(false),
            N_FLOAT32 = new Float32Type(true);
    private static final Float64Type FLOAT64 = new Float64Type(false),
            N_FLOAT64 = new Float64Type(true);
    private static final DateType DATE = new DateType(false), N_DATE = new DateType(true);
    private static final StringType STRING = new StringType(false), N_STRING = new StringType(true);
    private static final BinaryType BINARY = new BinaryType(false), N_BINARY = new BinaryType(true);

    public static BooleanType bool() {
        return BOOLEAN;
    }

    static BooleanType booleanType(boolean n) {
        return n ? N_BOOLEAN : BOOLEAN;
    }

    public static Int8Type int8() {
        return INT8;
    }

    static Int8Type int8(boolean n) {
        return n ? N_INT8 : INT8;
    }

    public static Int16Type int16() {
        return INT16;
    }

    static Int16Type int16(boolean n) {
        return n ? N_INT16 : INT16;
    }

    public static Int32Type int32() {
        return INT32;
    }

    static Int32Type int32(boolean n) {
        return n ? N_INT32 : INT32;
    }

    public static Int64Type int64() {
        return INT64;
    }

    static Int64Type int64(boolean n) {
        return n ? N_INT64 : INT64;
    }

    public static Float32Type float32() {
        return FLOAT32;
    }

    static Float32Type float32(boolean n) {
        return n ? N_FLOAT32 : FLOAT32;
    }

    public static Float64Type float64() {
        return FLOAT64;
    }

    static Float64Type float64(boolean n) {
        return n ? N_FLOAT64 : FLOAT64;
    }

    public static DateType date() {
        return DATE;
    }

    static DateType date(boolean n) {
        return n ? N_DATE : DATE;
    }

    public static StringType string() {
        return STRING;
    }

    static StringType string(boolean n) {
        return n ? N_STRING : STRING;
    }

    public static BinaryType binary() {
        return BINARY;
    }

    static BinaryType binary(boolean n) {
        return n ? N_BINARY : BINARY;
    }

    public static DecimalType decimal(int precision, int scale) {
        return new DecimalType(precision, scale, false);
    }

    public static TimeType time(int precision) {
        return new TimeType(precision, false);
    }

    public static TimestampType timestamp(int precision, TimestampKind timestampKind) {
        return new TimestampType(precision, timestampKind, false);
    }

    public static ListType list(LogicalType elementType) {
        return new ListType(elementType, false);
    }

    public static MapType map(LogicalType keyType, LogicalType valueType) {
        return new MapType(keyType, valueType, false);
    }

    public static StructType struct(RecordType recordType) {
        return new StructType(recordType, false);
    }

    public static ExtensionType extension(
            String typeId, String parametersJson, LogicalType physicalType) {
        return new ExtensionType(typeId, parametersJson, physicalType, false);
    }
}
