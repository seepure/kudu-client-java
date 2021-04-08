package org.seepure.kudu.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * AbstractKuduWriter is in charge of :
 *  <li>1. holding common properties
 *  <li>2. maintaining Single-Instance of {@link org.apache.kudu.client.KuduClient}
 *  <li>3. maintaining metadata (or say table schema)
 *  <li>4. providing basic method for kudu-insert operation.
 * </P>
 *
 * <p>
 * Life cycle:
 *  <li>1. init</li>
 *  <li>2. addRecord<li/>
 *  <li>3. flush if condition satisfied</li>
 *  <li>4. close </li>
 * </p>
 *
 * <p>Thread model:
 *  depends on sub-class implement
 * </p>
 */

public abstract class AbstractKuduWriter {
    private static Logger LOG = LoggerFactory.getLogger(AbstractKuduWriter.class);
    protected static final String DEFAULT_COMMIT_SIZE = "2400";
    protected static final String DEFAULT_BUFFERSPACE = "5000";

    static {
        DnsUtils.addDnsCache();
    }

    protected KuduClient client;
    protected KuduSession session;
    protected String masterIpPorts;
    protected String dbName;
    protected String tableName;
    protected KuduTable kuduTable;
    protected volatile Map<String, ColumnSchema> schemaMap;
    protected List<Map<String, String>> recordList;

    protected final int bufferSpace;
    protected final int commitSize;
    protected final boolean sortByKeyEnable;
    protected final String sortKey;
    protected AtomicLong counter;


    protected Properties properties;

    public AbstractKuduWriter(Properties properties) {
        this.properties = properties;
        masterIpPorts = properties.getProperty(KuduConfConst.KUDU_MASTER_IP_PORTS);
        dbName = properties.getProperty(KuduConfConst.KUDU_DB_NAME);
        tableName = properties.getProperty(KuduConfConst.KUDU_TABLE_NAME);
        if (StringUtils.isNotBlank(dbName)) {
            tableName = dbName + "." + tableName;
        }
        commitSize = Integer.parseInt(properties.getProperty(KuduConfConst.KUDU_COMMIT_SIZE, DEFAULT_COMMIT_SIZE));
        bufferSpace = Integer.parseInt(properties.getProperty(KuduConfConst.KUDU_BUFFER_SPACE, DEFAULT_BUFFERSPACE));

        sortByKeyEnable = Boolean.parseBoolean(properties.getProperty(KuduConfConst.WRITE_SORT_BY_KEY_ENABLE,"false"));
        sortKey = properties.getProperty(KuduConfConst.SORT_KEY, "event_time");

        recordList = new ArrayList<Map<String, String>>();
    }

    public void init() throws Exception {
        //todo 打印必要的配置信息
        LOG.info(getClass().getSimpleName() + " is initializing ...");
        LOG.info("config properties: " + properties.toString());
        LOG.info(String.format("master_ip_ports: %s, table_name: %s", masterIpPorts, tableName));
        LOG.info(String.format("commitSize: %d, bufferSpace: %d", commitSize, bufferSpace));

        if (client == null) {
            client = new KuduClient.KuduClientBuilder(masterIpPorts).build();
            LOG.info("KuduClient has been built. ");
        }
        kuduTable = client.openTable(tableName);
        Schema schema = kuduTable.getSchema();
        schemaMap = new HashMap<String, ColumnSchema>();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            schemaMap.put(columnSchema.getName(), columnSchema);
        }
        LOG.info("KuduClient has open table: " + tableName + ", schemaMap: " + schemaMap.toString());
    }

    public void close() throws KuduException {
        if (client != null) {
            client.close();
            LOG.info("close kudu client");
        }
    }

    public abstract void addRecord(Map<String, String> record) throws KuduException;

    public abstract void flush() throws KuduException;

    public Map<String, ColumnSchema> getSchemaMap() {
        return Collections.unmodifiableMap(schemaMap);
    }

    protected Upsert getUpsert(Map<String, String> value, KuduTable table) {
        Upsert upsert = table.newUpsert();
        table.newUpdate();
        PartialRow row = upsert.getRow();
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert into KuduInsert with map: " + value);
        }

        for (String colName : value.keySet()) {
            Object val = value.get(colName);
            ColumnSchema type = schemaMap.get(colName);
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
                default:
                    break;
            }
        }
        return upsert;
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

    public List<Map<String, String>> getRecordList() {
        return recordList;
    }

    public void setRecordList(List<Map<String, String>> recordList) {
        this.recordList = recordList;
    }
}
