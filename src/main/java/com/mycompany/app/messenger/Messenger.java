package com.mycompany.app.messenger;

import com.mycompany.app.auth.Auth;
import com.mycompany.app.config.Config;
import com.mycompany.app.kafka.TopicManager;
import com.sun.org.apache.xpath.internal.operations.Bool;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.common.internals.Topic;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.Collections;
import com.mycompany.app.kafka.Producer;
import com.mycompany.app.kafka.Consumer;
import org.json.zip.None;


public class Messenger {



    Logger mLogger = Logger.getLogger(Messenger.class);
    private List<String> mTenants;
    private Map<String, Map<String, List<BiFunction>>> mEventCallbacks;
    private Map<String, Map<String,String>> mSubjects;
    private Map<String, Map<String,String>> mGlobalSubjects;
    private Map<String, Map<String,String>> mTopics;
    private Map<String, Map<String,String>> mProducerTopics;
    private Consumer mConsumer;

    public Messenger(){
        mTenants = new ArrayList<>();
        mEventCallbacks = new HashMap<>();
        mSubjects = new HashMap<>();
        mGlobalSubjects = new HashMap<>();
        mTopics = new HashMap<>();
        mProducerTopics = new HashMap<>();

        Producer mProducer = new Producer(null);
        mConsumer = new Consumer( (ten,msg) -> { this.processKafkaMessages(ten,msg);  return 0;});
        this.createChannel(Config.getInstance().getTenancyManagerDefaultSubject(),"rw",true);
        };


    public void init(){
        this.on(Config.getInstance().getTenancyManagerDefaultSubject(), "message", (ten, msg) -> {this.processNewTenant(ten,msg); return null;});
        ArrayList<String> retTenants = Auth.getInstance().getTenants();
        if(retTenants.isEmpty()){
            System.out.println("Cannot initialize messenger, as the list of tenants could not be retrieved. Bailing out");
            return;
        }
        System.out.println("Got list of tenants");
        for(String ten : retTenants) {
            System.out.println("Bootstraping tenant: " + ten);
            String newTenantMsg = "{\"tenant\": " + ten + "}";
            this.processNewTenant(Config.getInstance().getInternalTenant(), newTenantMsg);
        }
    }

    private void processNewTenant(String tenant, String msg){
        System.out.println("Received message in tenancy subject");
        System.out.println("Message is: " + msg);


        JSONObject jsonObj = new JSONObject(msg);

        if(jsonObj.isNull(tenant)){
            System.out.println("Received message is invalid");
            return;
        }
        String newTenant = jsonObj.get("tenant").toString();
        if(this.mTenants.contains(newTenant)){
            System.out.println("Tenant already exists...");
            return;
        }

        this.mTenants.add(newTenant);

        for(String sub: this.mSubjects.keySet()){
            this.bootstrapTenants(sub, newTenant, this.mSubjects.get(sub).get("mode"),false);
        }
        this.emit(Config.getInstance().getTenancyManagerDefaultSubject(), Config.getInstance().getInternalTenant(), "new-tenant", newTenant);
    }

    public void on(String subject, String event, BiFunction<String, String, Void> callback){
        System.out.println("Registering a new callback for subject " + subject + " and event " + event);

        if (!this.mEventCallbacks.containsKey(subject)){
            this.mEventCallbacks.put(subject, new HashMap());
        }
        if(!this.mEventCallbacks.get(subject).containsKey(event)){
            this.mEventCallbacks.get(subject).put(event, new ArrayList());
        }

        this.mEventCallbacks.get(subject).get(event).add(callback);
    }

    private void bootstrapTenants(String subject, String tenant, String mode, boolean isGlobal){
        System.out.println("Bootstraping tenant: " + tenant + "for subject: " + subject);
        System.out.println("Global: " + isGlobal + "and mode: " + mode);

        try {
            String retTopic = TopicManager.getInstance().getTopic(subject, tenant, isGlobal);
            if (retTopic.isEmpty()){
                System.out.println("Could not retrieve topic...");
                return;
            }
            if (this.mTopics.containsKey(retTopic)){
                System.out.println("Already had a topic for " + subject + "@" + tenant);
                return;
            }

            System.out.println("Got topic for " + subject + "@" + tenant);
            System.out.println("Topic is: " + retTopic);
            this.mTopics.put(retTopic, new HashMap());
            this.mTopics.get(retTopic).put("tenant", tenant);
            this.mTopics.get(retTopic).put("subject", subject);

            if (mode.contains("r")){
                System.out.println("Telling consumer to subscribe to a new topic...");
                this.mConsumer.subscribe(retTopic);
                if(this.mTopics.size() == 1) {
                    System.out.println("Starting Consumer thread...");
                    try {
                        this.mConsumer.run();
                        System.out.println("... started consumer thread successfully");
                    } catch (Exception error) {
                        System.out.println("Could not start consumer");
                    }
                } else {
                    System.out.println("Consumer thread is already started");
                }
            }

            if (mode.contains("w")) {
                System.out.println("Adding a producer topic..");
                if (!this.mProducerTopics.containsKey(subject)) {
                    this.mProducerTopics.put(subject, new HashMap());
                    this.mProducerTopics.get(subject).put(tenant, retTopic);
                }
            }
        } catch(Exception error) {
            System.out.println("Something went wrong while bootstraping the tenant: " + error);
        }
    }

    public void createChannel(String subject, String mode, Boolean isGlobal) {
        System.out.println("Creating channel for: " + subject);

        List<String> associatedTenants = new ArrayList<>();

        if (isGlobal) {
            associatedTenants.add(Config.getInstance().getInternalTenant());
            this.mGlobalSubjects.put(subject,new HashMap());
            this.mGlobalSubjects.get(subject).put("mode", mode);
        } else {
            Collections.copy(associatedTenants,this.mTenants);
            this.mSubjects.put(subject,new HashMap());
            this.mSubjects.get(subject).put("mode", mode);
        }
        System.out.println("and tenants: ");
        for (String ten : associatedTenants){
            System.out.println(ten);
            this.bootstrapTenants(subject, ten, mode, isGlobal);
        }

    }

    private void processKafkaMessages(String topic, String message){
        System.out.println("topic is: " + topic + " message is : " + message);

        this.emit(this.mTopics.get(topic).get("subject"), this.mTopics.get(topic).get("tenant"), "message", message);
    }

    private void emit(String subject, String tenant, String event, String data){
        System.out.println("Emitting new event " + event + "for subject " + subject + "@" + tenant);

        if(!this.mEventCallbacks.containsKey(subject)){
            System.out.println("No one is listening to " + subject + " events");
            return;
        }
        if(!this.mEventCallbacks.get(subject).containsKey(event)){
            System.out.println("No one is listening to " + subject + " " + event + " events");
            return;
        }

        for (BiFunction fun : mEventCallbacks.get(subject).get(event)){
            fun.apply(tenant, data);
        }
    }


    public static void main (String[] args){
        Messenger msg = new Messenger();
        msg.init();
//        msg.on("device-data", "admin", (a,b) -> {System.out.println(a + b); return null;});


    }

}
