package org.seepure.kudu.client;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    public static void main(String[] args) throws KuduException {
        System.out.println("hahaha");
        if (args == null || args.length != 2) {
            System.err.println("usage: java -jar xxx.jar upsert/delete col1=val1,col2=val2,col3=val3,... ");
        }
        String opType = args[0];
        Map<String, String> record = getRecordFromArgs(args[1]);
        String tableName = "t_kudu_client_test";
        String dbName = "impala::beacon_kudu_new_test";
        String masters = "kuduset09-master1:7051,kuduset09-master2:7051,kuduset09-master3:7051";
        Properties properties = new Properties();
        properties.setProperty(KuduConfConst.KUDU_MASTER_IP_PORTS, masters);
        properties.setProperty(KuduConfConst.KUDU_DB_NAME, dbName);
        BeaconKuduClient beaconKuduClient = new BeaconKuduManualClient(properties);
        switch (opType.toLowerCase()) {
            case "upsert":
                beaconKuduClient.upsert(tableName, record);
                break;
            case "delete":
                beaconKuduClient.delete(tableName, record);
                break;
        }

        beaconKuduClient.close();
    }

    private static Map<String, String> getRecordFromArgs(String arg) {
        Map<String, String> record = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(arg)) {
            LOG.debug("arg: "+ arg);
            String[] kvs = StringUtils.split(arg, ',');
            LOG.debug("kvs.length = " + kvs.length);
            for (String kv : kvs) {
                String[] kvPair = StringUtils.split(kv, '=');
                record.put(kvPair[0], kvPair[1]);
            }
        }
        return record;
    }


}
