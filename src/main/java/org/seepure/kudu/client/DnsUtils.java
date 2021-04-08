package org.seepure.kudu.client;

import com.alibaba.dcm.DnsCacheManipulator;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DnsUtils {

    private static Logger LOG = LoggerFactory.getLogger(DnsUtils.class);

    public static void addDnsCache() {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("all_kudu.txt")));
        String line = null;
        try {
            while ((line = br.readLine()) != null) {
                LOG.info(line);
                String[] split = StringUtils.split(line, ' ');
                if (split != null && split.length == 2) {
                    DnsCacheManipulator.setDnsCache(split[1], split[0]);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
