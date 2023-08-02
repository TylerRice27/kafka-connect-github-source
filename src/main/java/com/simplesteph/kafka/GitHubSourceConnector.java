package com.simplesteph.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GitHubSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(GitHubSourceConnector.class);
    private GitHubSourceConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        config = new GitHubSourceConnectorConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GitHubSourceTask.class;
    }

    // Note tasksConfigs is where you will add multiple tasks to your connector.
    // Also this is probably most of where your logic will go
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        // Define the individual task configurations that will be executed.
        ArrayList<Map<String, String>> configs = new ArrayList<>(1);
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public void stop() {
        // Do things that are necessary to stop your connector.
        // nothing is necessary to stop for this connector
        // NOTE a good thing to do might be to STOP/CLOSE a database connection when you
        // call stop
        // in your code
    }

    @Override
    public ConfigDef config() {
        return GitHubSourceConnectorConfig.conf();
    }
}
