package org.seepure.kudu.client;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.seepure.kudu.client.vo.KuduOpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeaconKuduManualClient extends BeaconKuduClient {

    private static Logger LOG = LoggerFactory.getLogger(BeaconKuduManualClient.class);

    protected volatile KuduSession session;

    public BeaconKuduManualClient(Properties properties) throws KuduException {
        super(properties);
    }

    @Override
    protected void init() throws KuduException {
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(bufferSpace);
        LOG.info(getClass().getSimpleName() + " init finished. ");
    }

    @Override
    public KuduOpResult upsert(String tableName, Map<String, String> record) throws KuduException {
        if (record == null) {
            return null;
        }
        return upsert(tableName, Arrays.asList(record));
    }

    @Override
    public KuduOpResult upsert(String tableName, List<Map<String, String>> records) throws KuduException {
        if (CollectionUtils.isEmpty(records)) {
            return new KuduOpResult(200, "upsert ok.");
        }
        String physicalTableName = getPhysicalTableName(tableName);
        synchronized (this) {
            for (Map<String, String> record : records) {
                Upsert upsert = getUpsert(physicalTableName, record);
                session.apply(upsert);
            }
            List<OperationResponse> responses = session.flush();
        }
        return new KuduOpResult(200, "upsert ok.");
    }

    @Override
    public KuduOpResult delete(String tableName, Map<String, String> record) throws KuduException {
        if (record == null) {
            return null;
        }
        return delete(tableName, Arrays.asList(record));
    }

    @Override
    public KuduOpResult delete(String tableName, List<Map<String, String>> records) throws KuduException {
        if (CollectionUtils.isEmpty(records)) {
            return new KuduOpResult(200, "delete ok.");
        }
        String physicalTableName = getPhysicalTableName(tableName);
        synchronized (this) {
            for (Map<String, String> record : records) {
                Delete delete = getDelete(physicalTableName, record);
                session.apply(delete);
            }
            List<OperationResponse> responses = session.flush();
        }
        return new KuduOpResult(200, "delete ok.");
    }

    @Override
    public void close() {
        if (session != null) {
            try {
                session.flush();
                session.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        super.close();
    }

}
