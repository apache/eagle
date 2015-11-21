package org.apache.eagle.metric.manager;

import org.apache.eagle.metric.Metric;
import org.apache.eagle.metric.report.MetricReport;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EagleMetricReportManager {

    private static EagleMetricReportManager manager = new EagleMetricReportManager();
    private Map<String, MetricReport> metricReportMap = new ConcurrentHashMap<>();

    private EagleMetricReportManager() {

    }

    public static EagleMetricReportManager getInstance () {
        return manager;
    }

    public boolean register(String name, MetricReport report) {
       if (metricReportMap.get(name) == null) {
           synchronized (metricReportMap) {
               if (metricReportMap.get(name) == null) {
                   metricReportMap.put(name, report);
                   return true;
               }
            }
        }
        return false;
    }

    public Map<String, MetricReport> getRegisteredReports() {
        return metricReportMap;
    }

    public void emit(List<Metric> list) {
        synchronized (this.metricReportMap) {
            for (MetricReport report : metricReportMap.values()) {
                report.emit(list);
            }
        }
    }
}
