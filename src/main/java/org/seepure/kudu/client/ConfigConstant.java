package org.seepure.kudu.client;

public class ConfigConstant {

    public static final String KUDU_MASTER_IP_PORTS = "kudu.master_ip_ports";
    public static final String KUDU_DB_NAME = "kudu.db_name";
    public static final String KUDU_TABLE_NAME = "kudu.table_name";
    public static final String KUDU_COMMIT_SIZE = "kudu.commit_size";
    public static final String KUDU_BUFFER_SPACE = "kudu.buffer_space";
    public static final String WRITE_SORT_BY_KEY_ENABLE = "kudu.write_sort_by_key";

    public static final String SORT_KEY = "kudu.sort_key";
    public static final String AUTO_UPDATE_SCHEMA = "kudu.auto_update_schema";

    //only useful fro KuduManualFlushWriter
    public static final String ASYNC_FLUSH = "kudu.async_flush";
    public static final String FLUSH_INTERVAL = "kudu.flush_interval";
    public static final String KUDU_WRITER_MODE = "kudu.write_mode";
    public static final String KUDU_WRITER_MODE_MANUAL_FLUSH = "manual_flush";
    public static final String KUDU_WRITER_MODE_AUTO_FLUSH_BACKGROUND = "auto_flush_background";

    public static final String CORRECT_RANGE_PARTITION = "kudu.correct_range_partition";
    public static final String RANGE_PARTITION_KEY = "kudu.range_partition_key";
    public static final String AUTO_UPDATE_SCHEMA_INTERVAL = "kudu.auto_update_schema_interval";
    public static final String MONITOR_INTERVAL = "kudu.monitor_interval";
}
