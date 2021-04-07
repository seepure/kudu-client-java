package org.seepure.kudu.client;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.SessionConfiguration;
import org.seepure.kudu.client.vo.KuduOpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeaconKuduManualClient extends BeaconKuduClient {

    private static Logger LOG = LoggerFactory.getLogger(BeaconKuduManualClient.class);

    public BeaconKuduManualClient(Properties properties) throws KuduException {
        super(properties);
    }

    @Override
    protected void init() throws KuduException {
        super.init();
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(bufferSpace);
        LOG.info(getClass().getSimpleName() + " init finished. ");
    }

    @Override
    public KuduOpResult upsert(Map<String, String> record) {
        return null;
    }

    @Override
    public KuduOpResult upsert(List<Map<String, String>> records) {
        return null;
    }

    @Override
    public KuduOpResult delete(Map<String, String> record) {
        return null;
    }

    @Override
    public KuduOpResult delete(List<Map<String, String>> records) {
        return null;
    }
}
