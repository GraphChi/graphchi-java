package com.twitter.pers;

import com.sun.xml.internal.org.jvnet.fastinfoset.FastInfosetException;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Properties;

/**
 * Class for managing experiments/
 */
public class Experiment {

private Properties props;

    public Experiment(String expFile) {
        try {
            props = new Properties();
            props.load(new FileReader(expFile));

            System.out.println(props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getGraph() {
        return getFilenameProperty("graph");
    }

    public int getNumShards() {
        return Integer.parseInt(props.getProperty("shards"));
    }

    public int getNumIterations() {
        String iters = props.getProperty("iterations");
        return Integer.parseInt(iters);
    }

    public String getId() {
        return props.getProperty("id");
    }

    public String getProperty(String k) {
        return props.getProperty(k);
    }

    public String getFilenameProperty(String key) {
        String s = getProperty(key);
        return s.replace("~", System.getProperty("user.home"));
    }

    public String getOutputName(HashMap<String, String> placeholders) {
        String template = props.getProperty("outputfile");
        for(String key : placeholders.keySet()) {
            template = template.replace("$" + key, placeholders.get(key));
        }
        for(String key : props.stringPropertyNames()) {
            template = template.replace("$" + key, props.getProperty(key));
        }
        return template.replace("~", System.getProperty("user.home"));
    }
}
