package org.apache.eagle.app.example;

import org.apache.eagle.app.Configuration;

public class ExampleStormConfig extends Configuration {
    private int spoutNum = 1;

    public ExampleStormConfig(){}
    public ExampleStormConfig(String appName){
        this.setAppId(appName);
    }

    public int getSpoutNum() {
        return spoutNum;
    }

    public void setSpoutNum(int spoutNum) {
        this.spoutNum = spoutNum;
    }
}