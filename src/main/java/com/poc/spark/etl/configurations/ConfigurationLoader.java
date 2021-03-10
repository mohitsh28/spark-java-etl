package com.poc.spark.etl.configurations;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ConfigurationLoader implements Serializable {
    private static final Properties prop = new Properties();
    private static final Properties properties = new Properties();
    private static Map propertiesMap = new HashMap<>();

    private ConfigurationLoader() {
    }

    public static Map<String, String> getPropertiesMap() {
        String propFileName = "application.properties";
        InputStream inputStream = ConfigurationLoader.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            try {
                properties.load(inputStream);
                propertiesMap = properties;
            } catch (IOException e) {
                log.error("UNABLE_TO_LOAD_APP_PROPERTIES");
            }
        }
        return propertiesMap;
    }

    public static Map<String, String> getApplicationPropertiesMap(String configurationURL,String env) {
        if (env.equalsIgnoreCase("dev") || env.equalsIgnoreCase("local"))
            return getPropertiesMap();
        try {
            String fileURL = configurationURL;
            URL url = new URL(fileURL);
            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = httpURLConnection.getInputStream();
                Gson g = new Gson();
                Map x = g.fromJson(IOUtils.toString(inputStream,StandardCharsets.ISO_8859_1),Map.class);
                log.info("LogStatements");
                propertiesMap = (Map) ((LinkedTreeMap) ((ArrayList) x.get("propertySources")).get(0)).get("source");
            }
        } catch (IOException e) {
            log.error(e.getMessage(),e);
        }
        return propertiesMap;
    }
}