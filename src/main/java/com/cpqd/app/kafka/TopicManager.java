package com.cpqd.app.kafka;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.cpqd.app.auth.Auth;
import com.cpqd.app.config.Config;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

public class TopicManager {

    private Map <String, String> mTopics;
    private static TopicManager mInstance;

    public TopicManager(){
        this.mTopics = new HashMap<>();
    }

    public static synchronized TopicManager getInstance() {
        if (mInstance == null) {
            mInstance = new TopicManager();
        }
        return mInstance;
    }

    /**
     * Get the topic given tenant:subject.
     *
     * @param subject Subject of the topic.
     * @param tenant Tenant of the topic.
     * @param global If this subject is or not global.
     * @return The topic related to tenant:subject.
     */
    public String getTopic(String subject, String tenant, Boolean global){
        StringBuilder key = new StringBuilder(tenant);
        key.append(":" + subject);

        if(this.mTopics.containsKey(key.toString())){
            return this.mTopics.get((key.toString()));
        }

        StringBuilder url = new StringBuilder(Config.getInstance().getDataBrokerAddress());
        url.append("/topic/" + subject);
        url.append(global ? "?global=true" : "");

        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString()).header("authorization","Bearer " + Auth.getInstance().getToken(tenant)).asJson();
            JSONObject jsonResponse = request.getBody().getObject();
            return jsonResponse.getString("topic");
        } catch(UnirestException exception) {
            System.out.println("ASD: " + exception);
            return "";
        } catch (JSONException exception){
            System.out.println("BBB");
            return "";
        }
    }
}
