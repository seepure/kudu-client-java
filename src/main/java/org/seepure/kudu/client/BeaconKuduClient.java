package org.seepure.kudu.client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.util.TimestampUtil;
import org.seepure.kudu.client.util.AssertUtil;
import org.seepure.kudu.client.vo.KuduOpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BeaconKuduClient {

    protected static final String DEFAULT_COMMIT_SIZE = "2400";
    protected static final String DEFAULT_BUFFERSPACE = "5000";
    protected static volatile KuduClient client;
    protected static Map<String, KuduTable> kuduTableMap = new ConcurrentHashMap<>();
    protected static volatile Map<String, Map<String, ColumnSchema>> schemaMap = new ConcurrentHashMap<>();
    private static Logger LOG = LoggerFactory.getLogger(BeaconKuduClient.class);

    static {
        DnsUtils.addDnsCache();
    }

    protected final int bufferSpace;
    protected final int commitSize;
    protected String masterIpPorts;
    protected String dbName;
    protected Properties properties;

    public BeaconKuduClient(Properties properties) throws KuduException {

        this.properties = properties;

        masterIpPorts = properties.getProperty(KuduConfConst.KUDU_MASTER_IP_PORTS);
        dbName = properties.getProperty(KuduConfConst.KUDU_DB_NAME);
        commitSize = Integer.parseInt(properties.getProperty(KuduConfConst.KUDU_COMMIT_SIZE, DEFAULT_COMMIT_SIZE));
        bufferSpace = Integer.parseInt(properties.getProperty(KuduConfConst.KUDU_BUFFER_SPACE, DEFAULT_BUFFERSPACE));

        LOG.info(getClass().getSimpleName() + " is initializing ...");
        LOG.info("config properties: " + properties.toString());
        LOG.info(String.format("master_ip_ports: %s", masterIpPorts));
        LOG.info(String.format("commitSize: %d, bufferSpace: %d", commitSize, bufferSpace));

        if (client == null) {
            synchronized (BeaconKuduClient.class) {
                if (client == null) {
                    client = new KuduClient.KuduClientBuilder(masterIpPorts).build();
                    LOG.info("KuduClient has been built. ");
                }
            }
        }

        init();
    }

    protected abstract void init() throws KuduException;

    public abstract KuduOpResult upsert(String tableName, Map<String, String> record) throws KuduException;

    public abstract KuduOpResult upsert(String tableName, List<Map<String, String>> records) throws KuduException;

    public abstract KuduOpResult delete(String tableName, Map<String, String> record) throws KuduException;

    public abstract KuduOpResult delete(String tableName, List<Map<String, String>> records) throws KuduException;

    public void close() {}

    public Map<String, ColumnSchema> getSchemaMap(String physicalTableName) {
        return schemaMap.get(physicalTableName);
    }

    public String getPhysicalTableName(String tableName) {
        AssertUtil.assertTrue(StringUtils.isNotBlank(tableName), "empty table name");
        return dbName + "." + tableName;
    }

    protected KuduTable getKuduTable(String physicalTableName) throws KuduException {
        KuduTable kuduTable = kuduTableMap.get(physicalTableName);
        //因为client.openTable(physicalTableName)是一个很重的元数据操作, 所以这里要防止多线程多次open一个kudu表
        if (kuduTable == null) {
            synchronized (BeaconKuduClient.class) {
                if (kuduTable == null) {
                    kuduTable = client.openTable(physicalTableName);
                    Schema schema = kuduTable.getSchema();
                    Map<String, ColumnSchema> tableSchema = new LinkedHashMap<>();
                    for (ColumnSchema columnSchema : schema.getColumns()) {
                        tableSchema.put(columnSchema.getName(), columnSchema);
                    }
                    kuduTableMap.put(physicalTableName, kuduTable);
                    BeaconKuduClient.schemaMap.put(physicalTableName, tableSchema);
                    String logInfo = "KuduClient has open table: " + physicalTableName + ", schemaMap: " + tableSchema
                            .toString();
                    LOG.info(logInfo);
                }
            }
        }
        return kuduTable;
    }

    protected Upsert getUpsert(String physicalTableName, Map<String, String> value) throws KuduException {
        KuduTable table = getKuduTable(physicalTableName);
        Map<String, ColumnSchema> tableSchema = getSchemaMap(physicalTableName);
        Upsert upsert = table.newUpsert();
        PartialRow row = upsert.getRow();
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert into KuduUpsert with map: " + value.toString());
        }

        for (String colName : value.keySet()) {
            Object val = value.get(colName);
            ColumnSchema type = tableSchema.get(colName);
            if (type == null) {
                continue;
            }
            switch (type.getType()) {
                case STRING:
                    row.addString(colName, val == null ? "" : String.valueOf(val));
                    break;
                case INT8:
                case INT16:
                case INT32:
                    addInt(row, colName, val);
                    break;
                case INT64:
                    addLong(row, colName, val);
                    break;
                case FLOAT:
                    addFloat(row, colName, val);
                    break;
                case DOUBLE:
                    addDouble(row, colName, val);
                    break;
                case UNIXTIME_MICROS:
                    addTimeStamp(row, colName, val);
                    break;
                default:
                    break;
            }
        }
        return upsert;
    }

    protected Delete getDelete(String physicalTableName, Map<String, String> value) throws KuduException {
        KuduTable table = getKuduTable(physicalTableName);
        Map<String, ColumnSchema> tableSchema = getSchemaMap(physicalTableName);
        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert into KuduDelete with map: " + value.toString());
        }

        for (String colName : value.keySet()) {
            Object val = value.get(colName);
            ColumnSchema type = tableSchema.get(colName);
            if (type == null) {
                continue;
            }
            switch (type.getType()) {
                case STRING:
                    row.addString(colName, val == null ? "" : String.valueOf(val));
                    break;
                case INT8:
                case INT16:
                case INT32:
                    addInt(row, colName, val);
                    break;
                case INT64:
                    addLong(row, colName, val);
                    break;
                case FLOAT:
                    addFloat(row, colName, val);
                    break;
                case DOUBLE:
                    addDouble(row, colName, val);
                    break;
                case UNIXTIME_MICROS:
                    addTimeStamp(row, colName, val);
                    break;
                default:
                    break;
            }
        }
        return delete;
    }

    private void addTimeStamp(PartialRow partialRow, String col, Object val) {
        if (val instanceof Long) {
            partialRow.addLong(col, (Long) val);
        } else {
            long micros = Long.parseLong(String.valueOf(val));
            partialRow
                    .addTimestamp(col, TimestampUtil.microsToTimestamp(micros));
        }
    }

    protected void addLong(PartialRow partialRow, String col, Object val) {
        if (val instanceof Long) {
            partialRow.addLong(col, (Long) val);
        } else {
            partialRow
                    .addLong(col, Long.parseLong(String.valueOf(val)));
        }
    }

    protected void addInt(PartialRow partialRow, String col, Object val) {
        if (val instanceof Integer) {
            partialRow.addInt(col, (Integer) val);
        } else {
            partialRow
                    .addInt(col, Integer.parseInt(String.valueOf(val)));
        }
    }

    protected void addDouble(PartialRow partialRow, String col, Object val) {
        Double dVal = null;
        if (val instanceof Long || val instanceof Integer) {
            dVal = ((Number) val).doubleValue();
        } else if (val instanceof Float) {
            dVal = ((Float) val).doubleValue();
        } else if (val instanceof Double) {
            dVal = (Double) val;
        } else {
            dVal = Double.parseDouble(String.valueOf(val));
        }
        partialRow.addDouble(col, dVal);
    }

    protected void addFloat(PartialRow partialRow, String col, Object val) {
        Float fVal = null;
        if (val instanceof Long || val instanceof Integer) {
            fVal = ((Number) val).floatValue();
        } else if (val instanceof Double) {
            fVal = ((Double) val).floatValue();
        } else if (val instanceof Float) {
            fVal = (Float) val;
        } else {
            fVal = Float.parseFloat(String.valueOf(val));
        }
        partialRow.addFloat(col, fVal);
    }

}
