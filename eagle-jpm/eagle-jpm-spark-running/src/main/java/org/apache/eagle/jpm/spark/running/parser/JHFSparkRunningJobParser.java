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

    private static final Logger logger = LoggerFactory.getLogger(JHFSparkParser.class);

    JHFSparkEventReader eventReader;

    public JHFSparkRunningJobParser(JHFSparkEventReader reader) {
        this.eventReader = reader;
    }

    @Override
    public void parse(InputStream is) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        try {
            String line = null;
            boolean stopReadline = false;
            while ((line = reader.readLine()) != null && !stopReadline) {
                try {
                    boolean isValid = true;
                    JSONObject eventObj = parseAndValidateJSON(line, isValid);

                    if (isValid) {
                        String eventType = (String) eventObj.get("Event");
                        logger.info("Event type: " + eventType);
                        this.eventReader.read(eventObj);
                        if (eventType.equals("SparkListenerEnvironmentUpdate")) {
                            stopReadline = true;
                        }
                    }
                } catch (Exception e) {
                    logger.error(String.format("Fail to parse %s.", line), e);
                }
            }
            this.eventReader.clearReader();
        } finally {
            reader.close();
        }
    }

    private JSONObject parseAndValidateJSON(String line, boolean isValid) {
        JSONObject eventObj = null;
        JSONParser parser = new JSONParser();
        try {
            eventObj = (JSONObject) parser.parse(line);
        } catch (ParseException ex) {
            isValid = false;
            logger.error(String.format("Invalid json string. Fail to parse %s.", line), ex);
        }
        return eventObj;
    }
}
