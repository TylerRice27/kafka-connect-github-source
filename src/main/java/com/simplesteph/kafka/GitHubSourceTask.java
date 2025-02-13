package com.simplesteph.kafka;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.simplesteph.kafka.model.Issue;
import com.simplesteph.kafka.model.PullRequest;
import com.simplesteph.kafka.model.User;
import com.simplesteph.kafka.utils.DateUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import static com.simplesteph.kafka.GitHubSchemas.*;

public class GitHubSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);
    public GitHubSourceConnectorConfig config;

    protected Instant nextQuerySince;
    protected Integer lastIssueNumber;
    protected Integer nextPageToVisit = 1;
    protected Instant lastUpdatedAt;

    GitHubAPIHttpClient gitHubHttpAPIClient;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        // Do things here that are required to start your task. This could be open a
        // connection to a database, etc.
        config = new GitHubSourceConnectorConfig(map);
        initializeLastVariables();
        gitHubHttpAPIClient = new GitHubAPIHttpClient(config);
    }

    Map<String, Object> lastSourceOffset;

    // 2nd Most important section of the code
    private void initializeLastVariables() {
        String jsonOffsetData = new Gson().toJson(lastSourceOffset);
        // TypeToken preserves the generic type information of the Map when
        // deserializing the JSON string.
        TypeToken<Map<String, Object>> typeToken = new TypeToken<Map<String, Object>>() {
        };
        Map<String, Object> offsetData = new Gson().fromJson(jsonOffsetData, typeToken.getType());
        if (offsetData == null) {
            // we haven't fetched anything yet, so we initialize to 7 days ago
            nextQuerySince = config.getSince();
            lastIssueNumber = -1;
        } else {
            Object updatedAt = offsetData.get(UPDATED_AT_FIELD);
            Object issueNumber = offsetData.get(NUMBER_FIELD);
            Object nextPage = offsetData.get(NEXT_PAGE_FIELD);
            if (updatedAt != null && (updatedAt instanceof String)) {
                nextQuerySince = Instant.parse((String) updatedAt);
            }
            if (issueNumber != null && (issueNumber instanceof String)) {
                lastIssueNumber = Integer.valueOf((String) issueNumber);
            }
            if (nextPage != null && (nextPage instanceof String)) {
                nextPageToVisit = Integer.valueOf((String) nextPage);
            }
        }
    }

    // Most important aspect of the code
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        gitHubHttpAPIClient.sleepIfNeed();

        // fetch data
        final ArrayList<SourceRecord> records = new ArrayList<>();
        JSONArray issues = gitHubHttpAPIClient.getNextIssues(nextPageToVisit, nextQuerySince);
        // we'll count how many results we get with i
        int i = 0;
        // A for loop that grabs each issue has a JSON object, then turns into a
        // sourceRecord and adds that
        // to my list of records array
        for (Object obj : issues) {
            Issue issue = Issue.fromJson((JSONObject) obj);
            SourceRecord sourceRecord = generateSourceRecord(issue);
            records.add(sourceRecord);
            i += 1;
            lastUpdatedAt = issue.getUpdatedAt();
        }
        if (i > 0)
            log.info(String.format("Fetched %s record(s)", i));
        if (i == 100) {
            // we have reached a full batch, we need to get the next one
            nextPageToVisit += 1;
        } else {
            nextQuerySince = lastUpdatedAt.plusSeconds(1);
            nextPageToVisit = 1;
            gitHubHttpAPIClient.sleep();
        }
        return records;
    }

    private SourceRecord generateSourceRecord(Issue issue) {
        return new SourceRecord(
                sourcePartition(),
                sourceOffset(issue.getUpdatedAt()),
                config.getTopic(),
                // NOTE the partition has to be null, don't set the partition yourself
                null, // partition will be inferred by the framework
                KEY_SCHEMA,
                buildRecordKey(issue),
                VALUE_SCHEMA,
                buildRecordValue(issue),
                issue.getUpdatedAt().toEpochMilli());
    }

    @Override
    public void stop() {
        // Do whatever is required to stop your task.
    }

    private Map<String, String> sourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(OWNER_FIELD, config.getOwnerConfig());
        map.put(REPOSITORY_FIELD, config.getRepoConfig());
        return map;
    }

    private Map<String, String> sourceOffset(Instant updatedAt) {
        Map<String, String> map = new HashMap<>();
        // The Value that was last updated or read
        map.put(UPDATED_AT_FIELD, DateUtils.MaxInstant(updatedAt, nextQuerySince).toString());
        // What message to read from next
        map.put(NEXT_PAGE_FIELD, nextPageToVisit.toString());
        return map;
    }

    private Struct buildRecordKey(Issue issue) {
        // Key Schema
        Struct key = new Struct(KEY_SCHEMA)
                .put(OWNER_FIELD, config.getOwnerConfig())
                .put(REPOSITORY_FIELD, config.getRepoConfig())
                .put(NUMBER_FIELD, issue.getNumber());

        return key;
    }

    public Struct buildRecordValue(Issue issue) {

        // Issue top level fields
        Struct valueStruct = new Struct(VALUE_SCHEMA)
                .put(URL_FIELD, issue.getUrl())
                .put(TITLE_FIELD, issue.getTitle())
                .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(NUMBER_FIELD, issue.getNumber())
                .put(STATE_FIELD, issue.getState());

        // User is mandatory
        User user = issue.getUser();
        Struct userStruct = new Struct(USER_SCHEMA)
                .put(USER_URL_FIELD, user.getUrl())
                .put(USER_ID_FIELD, user.getId())
                .put(USER_LOGIN_FIELD, user.getLogin());
        valueStruct.put(USER_FIELD, userStruct);

        // Pull request is optional
        PullRequest pullRequest = issue.getPullRequest();
        if (pullRequest != null) {
            Struct prStruct = new Struct(PR_SCHEMA)
                    .put(PR_URL_FIELD, pullRequest.getUrl())
                    .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
            valueStruct.put(PR_FIELD, prStruct);
        }

        return valueStruct;
    }

}