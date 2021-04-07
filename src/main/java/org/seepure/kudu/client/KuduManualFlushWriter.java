package org.seepure.kudu.client;

import static org.seepure.kudu.client.ConfigConstant.FLUSH_INTERVAL;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A AbstractKuduWriter using SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND.
 * <p>
 * Thread model: there is two thread:
 * <li>1. add record invoked by streaming-engine thread. </li>
 * <li>2. flushing thread running in background of AsyncKuduSession. </li>
 * </p>
 */

public class KuduManualFlushWriter extends AbstractKuduWriter {
    public static Logger logger = LoggerFactory.getLogger(KuduManualFlushWriter.class);
    private final long flushInterval;
    private long lastFlushTime = System.currentTimeMillis();

    public KuduManualFlushWriter(Properties properties) throws Exception {
        super(properties);
        flushInterval = Long.parseLong(properties.getProperty(FLUSH_INTERVAL, "1000"));
        init();
    }

    @Override
    public void init() throws Exception {
        //todo 打印必要的配置信息
        super.init();
        session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(bufferSpace);
        logger.info(getClass().getSimpleName() + " init finished. ");
    }

    @Override
    public void close() throws KuduException {
        session.flush();
        session.close();
        super.close();
    }

    @Override
    public void addRecord(Map<String, String> record) throws KuduException {
        if (record == null || record.size() < 1) {
            return;
        }
        recordList.add(record);
        if (shouldFlush()) {
            flush();
        }
    }

    private boolean shouldFlush() {
        long currentTime = System.currentTimeMillis();
        boolean result = recordList.size() >= commitSize || currentTime - lastFlushTime >= flushInterval;
        lastFlushTime = currentTime;
        return result;
    }

    @Override
    public void flush() throws KuduException {
        if (sortByKeyEnable) {
            sortRecordList(recordList);
        }
        Integer toInsertCount = createKuduInsertAndApply(recordList);
        long startTime = System.currentTimeMillis();
        List<OperationResponse> responses = session.flush();
        long endTime = System.currentTimeMillis();
        final long timeUse = endTime - startTime;
        logger.info(String.format("flushCount %s ,recordSize %s ,responsesSize %s, "
                + "time_use: %s", toInsertCount, recordList.size()
                + "", responses.size() + "", timeUse + ""));
        recordList.clear();
    }

    // 对数据进行排序
    private void sortRecordList(List<Map<String, String>> recordList) {
        if (CollectionUtils.isNotEmpty(recordList)) {
            long sortStart = System.nanoTime();
            recordList.sort(new Comparator<Map<String, String>>() {
                @Override
                public int compare(Map<String, String> o1, Map<String, String> o2) {
                    String sortKey1 = o1.getOrDefault(sortKey, "");
                    String sortKey2 = o2.getOrDefault(sortKey, "");
                    return sortKey1.compareTo(sortKey2);
                }
            });
            long sortCostMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sortStart);
            logger.warn("sort record: " + recordList.size() + " items cost : " + sortCostMS + " ms");
        }
    }

    protected int createKuduInsertAndApply(List<Map<String, String>> recordList) throws KuduException {
        int failedCount = 0;
        for (Map<String, String> record : recordList) {
            try {
                Upsert upsert = getUpsert(record, kuduTable);
                session.apply(upsert);
            } catch (Throwable t) {
                ++failedCount;
                logger.error(t.getMessage(), t);
            }
        }
        return recordList.size() - failedCount;
    }

}
