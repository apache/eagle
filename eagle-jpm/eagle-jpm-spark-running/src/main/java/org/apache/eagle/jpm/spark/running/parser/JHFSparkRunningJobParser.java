package org.apache.eagle.jpm.spark.running.parser;

import org.apache.eagle.jpm.spark.crawl.JHFParserBase;
import org.apache.eagle.jpm.spark.crawl.JHFSparkEventReader;
import org.apache.eagle.jpm.spark.crawl.JHFSparkParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 */
public class JHFSparkRunningJobParser implements JHFParserBase {

    private static final Logger LOG = LoggerFactory.getLogger(JHFSparkParser.class);

    private boolean isValidJson;

    private JHFSparkEventReader eventReader;

    public JHFSparkRunningJobParser(JHFSparkEventReader reader) {
        this.eventReader = reader;
    }

    @Override
    public void parse(InputStream is) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line = null;
            boolean stopReadline = false;
            while ((line = reader.readLine()) != null && !stopReadline) {
                try {
                    isValidJson = true;
                    JSONObject eventObj = parseAndValidateJSON(line);

                    if (isValidJson) {
                        String eventType = (String) eventObj.get("Event");
                        LOG.info("Event type: " + eventType);
                        this.eventReader.read(eventObj);
                        if (eventType.equals("SparkListenerEnvironmentUpdate")) {
                            stopReadline = true;
                        }
                    }
                } catch (Exception e) {
                    LOG.error(String.format("Fail to parse %s.", line), e);
                }
            }
            this.eventReader.clearReader();
        }
    }

    private JSONObject parseAndValidateJSON(String line) {
        JSONObject eventObj = null;
        JSONParser parser = new JSONParser();
        try {
            eventObj = (JSONObject) parser.parse(line);
        } catch (ParseException ex) {
            isValidJson = false;
            LOG.error(String.format("Invalid json string. Fail to parse %s.", line), ex);
        }
        return eventObj;
    }
}
