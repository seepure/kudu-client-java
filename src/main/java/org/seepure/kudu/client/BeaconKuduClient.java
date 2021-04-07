package org.seepure.kudu.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.seepure.kudu.client.vo.KuduOpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BeaconKuduClient {

    private static Logger LOG = LoggerFactory.getLogger(BeaconKuduClient.class);
    protected static final String DEFAULT_COMMIT_SIZE = "2400";
    protected static final String DEFAULT_BUFFERSPACE = "5000";

    static {
        DnsUtils.addDnsCache();
    }

    protected static volatile KuduClient client;
    protected static volatile KuduTable kuduTable;
    protected static volatile Map<String, ColumnSchema> schemaMap;
    protected KuduSession session;
    protected String masterIpPorts;
    protected String dbName;
    protected String tableName;


    protected final int bufferSpace;
    protected final int commitSize;

    protected Properties properties;

    public BeaconKuduClient(Properties properties) throws KuduException {

        this.properties = properties;

        masterIpPorts = properties.getProperty(ConfigConstant.KUDU_MASTER_IP_PORTS);
        dbName = properties.getProperty(ConfigConstant.KUDU_DB_NAME);
        tableName = properties.getProperty(ConfigConstant.KUDU_TABLE_NAME);
        if (StringUtils.isNotBlank(dbName)) {
            tableName = dbName + "." + tableName;
        }
        commitSize = Integer.parseInt(properties.getProperty(ConfigConstant.KUDU_COMMIT_SIZE, DEFAULT_COMMIT_SIZE));
        bufferSpace = Integer.parseInt(properties.getProperty(ConfigConstant.KUDU_BUFFER_SPACE, DEFAULT_BUFFERSPACE));


        LOG.info(getClass().getSimpleName() + " is initializing ...");
        LOG.info("config properties: " + properties.toString());
        LOG.info(String.format("master_ip_ports: %s, table_name: %s", masterIpPorts, tableName));
        LOG.info(String.format("commitSize: %d, bufferSpace: %d", commitSize, bufferSpace));

        if (client == null) {
            synchronized (BeaconKuduClient.class) {
                if (client == null) {
                    client = new KuduClient.KuduClientBuilder(masterIpPorts).build();
                    LOG.info("KuduClient has been built. ");
                    kuduTable = client.openTable(tableName);
                    Schema schema = kuduTable.getSchema();
                    schemaMap = new HashMap<String, ColumnSchema>();
                    for (ColumnSchema columnSchema : schema.getColumns()) {
                        schemaMap.put(columnSchema.getName(), columnSchema);
                    }
                    LOG.info("KuduClient has open table: " + tableName + ", schemaMap: " + schemaMap.toString());
                }
            }
        }

        init();
    }

    protected void init() throws KuduException {

    }

    public abstract KuduOpResult upsert(Map<String, String> record);

    public abstract KuduOpResult upsert(List<Map<String, String>> records);

    public abstract KuduOpResult delete(Map<String, String> record);

    public abstract KuduOpResult delete(List<Map<String, String>> records);

    public Map<String, ColumnSchema> getSchemaMap() {
        return Collections.unmodifiableMap(schemaMap);
    }

    protected Upsert getUpsert(Map<String, String> value, KuduTable table) {
        Upsert upsert = table.newUpsert();
        table.newUpdate();
        PartialRow row = upsert.getRow();
        if (LOG.isDebugEnabled()) {
            LOG.debug("convert into KuduUpsert with map: " + value);
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

}
