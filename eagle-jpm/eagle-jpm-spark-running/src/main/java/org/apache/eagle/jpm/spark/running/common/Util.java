package org.apache.eagle.jpm.spark.running.common;

import org.apache.hadoop.fs.Path;

public class Util {
    public static String getAppAttemptLogName(String appId, String attemptId) {
        if (attemptId.equals("0")) {
            return appId;
        }
        return appId + "_" + attemptId;
    }

    public static Path getFilePath(String baseDir, String appAttemptLogName) {
        String attemptLogDir = baseDir + "/" + appAttemptLogName;
        return new Path(attemptLogDir);
    }
}
