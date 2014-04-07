package org.apache.cassandra.hive.cql3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

// https://github.com/bfemiano/accumulo-hive-storage-manager/wiki/Basic-Tutorial
public class Cql3SerDe implements SerDe {
    static Logger LOG = LoggerFactory.getLogger(Cql3SerDe.class);
    static final String HIVE_TYPE_DOUBLE = "double";
    static final String HIVE_TYPE_FLOAT = "float";
    static final String HIVE_TYPE_BOOLEAN = "boolean";
    static final String HIVE_TYPE_BIGINT = "bigint";
    static final String HIVE_TYPE_TINYINT = "tinyint";
    static final String HIVE_TYPE_SMALLINT = "smallint";
    static final String HIVE_TYPE_INT = "int";

    private final MapWritable cachedWritable = new MapWritable();

    private int fieldCount;
    private StructObjectInspector objectInspector;
    private List<String> columnNames;
    private List<String> columnTypesArray;
    private List<Object> row;

    @Override
    public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
        final String columnString = HiveCassandraUtils.getPropertyValue(tbl, HiveCassandraUtils.COLUMN_MAPPINGS, Constants.LIST_COLUMNS);
        if (!Strings.isNullOrEmpty(columnString))
            columnNames = Arrays.asList(columnString.split("[,:;]"));
        LOG.debug("column names in hive table: {}", columnNames);
        fieldCount = columnNames.size();

        String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        if (!Strings.isNullOrEmpty(columnTypeProperty))
            columnTypesArray = Arrays.asList(columnTypeProperty.split("[,:;]"));
        LOG.debug("column types in hive table: {}", columnTypesArray);

        final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
            } else if (Cql3SerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray.get(i))) {
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            } else {
                // treat as string
                fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }
        }
        objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);
        row = new ArrayList<Object>(fieldCount);
    }

    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        if (!(wr instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable, received " + wr.getClass().getName());
        }

        final MapWritable input = (MapWritable) wr;
        final Text t = new Text();
        row.clear();

        for (int i = 0; i < fieldCount; i++) {
            t.set(columnNames.get(i));
            final Writable value = input.get(t);
            if (value != null && !NullWritable.get().equals(value)) {
                // parse as double to avoid NumberFormatException...
                // TODO:need more test,especially for type 'bigint'
                if (HIVE_TYPE_INT.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Double.valueOf(value.toString()).intValue());
                } else if (Cql3SerDe.HIVE_TYPE_SMALLINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Double.valueOf(value.toString()).shortValue());
                } else if (Cql3SerDe.HIVE_TYPE_TINYINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Double.valueOf(value.toString()).byteValue());
                } else if (Cql3SerDe.HIVE_TYPE_BIGINT.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Long.valueOf(value.toString()));
                } else if (Cql3SerDe.HIVE_TYPE_BOOLEAN.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Boolean.valueOf(value.toString()));
                } else if (Cql3SerDe.HIVE_TYPE_FLOAT.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Double.valueOf(value.toString()).floatValue());
                } else if (Cql3SerDe.HIVE_TYPE_DOUBLE.equalsIgnoreCase(columnTypesArray.get(i))) {
                    row.add(Double.valueOf(value.toString()));
                } else {
                    row.add(value.toString());
                }
            } else {
                row.add(null);
            }
        }

        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public Writable serialize(final Object obj, final ObjectInspector inspector) throws SerDeException {
        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();
        if (fields.size() != columnNames.size()) {
            throw new SerDeException(String.format("Required %d columns, received %d.", columnNames.size(), fields.size()));
        }

        cachedWritable.clear();
        for (int c = 0; c < fieldCount; c++) {
            StructField structField = fields.get(c);
            if (structField != null) {
                final Object field = structInspector.getStructFieldData(obj, fields.get(c));

                // TODO:currently only support hive primitive type
                final AbstractPrimitiveObjectInspector fieldOI = (AbstractPrimitiveObjectInspector) fields.get(c).getFieldObjectInspector();

                Writable value = (Writable) fieldOI.getPrimitiveWritableObject(field);

                if (value == null) {
                    if (PrimitiveCategory.STRING.equals(fieldOI.getPrimitiveCategory())) {
                        value = NullWritable.get();
                        // value = new Text("");
                    } else {
                        // TODO: now all treat as number
                        value = new IntWritable(0);
                    }
                }
                cachedWritable.put(new Text(columnNames.get(c)), value);
            }
        }
        return cachedWritable;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return new SerDeStats();
    }

}
