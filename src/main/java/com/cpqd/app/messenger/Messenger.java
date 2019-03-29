package com.cpqd.app.messenger;

import com.cpqd.app.auth.Auth;
import com.cpqd.app.config.Config;
import com.cpqd.app.kafka.TopicManager;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import com.cpqd.app.kafka.Producer;
import com.cpqd.app.kafka.Consumer;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Class responsible for sending and receiving messages through Kafka using
 * dojot subjects and tenants.
 */
public class Messenger {

    private ArrayList<String> mTenants;
    private Map<String, Map<String, List<BiFunction>>> mEventCallbacks;
    private Map<String, Map<String,String>> mSubjects;
    private Map<String, Map<String,String>> mGlobalSubjects;
    private Map<String, Map<String,String>> mTopics;
    private Map<String, Map<String,String>> mProducerTopics;
    private Consumer mConsumer;
    private Producer mProducer;

    public Messenger(){
        mTenants = new ArrayList<>();
        mEventCallbacks = new HashMap<>();
        mSubjects = new HashMap<>();
        mGlobalSubjects = new HashMap<>();
        mTopics = new HashMap<>();
        mProducerTopics = new HashMap<>();

        mProducer = new Producer();
        mConsumer = new Consumer( (ten,msg) -> { this.processKafkaMessages(ten,msg);  return 0;});

        };

    /**
     *  Initializes the messenger and sets with all tenants
     *
     */
    public void init(){
        this.createChannel(Config.getInstance().getTenancyManagerDefaultSubject(),"rw",true);
        this.on(Config.getInstance().getTenancyManagerDefaultSubject(), "message", (ten, msg) -> {this.processNewTenant(ten,msg); return null;});
        ArrayList<String> retTenants = Auth.getInstance().getTenants();
        if(retTenants.isEmpty()){
            System.out.println("Cannot initialize messenger, as the list of tenants could not be retrieved. Bailing out");
            throw new Error("Could not retrieve tenants");
        }
        for (String ten : retTenants){
            this.mTenants.add(ten);
        }
        System.out.println("Got list of tenants");
        for(String ten : retTenants) {
            System.out.println("Bootstraping tenant: " + ten);
            String newTenantMsg = "{\"tenant\": " + ten + "}";
            this.processNewTenant(Config.getInstance().getInternalTenant(), newTenantMsg);
        }
    }

    /**
     * Process new tenant: bootstrap it for all subjects registered and emit
     * an event.
     *
     * @param tenant The tenant associated to the message (NOT NEW TENANT).
     * @param msg The message just received with the new tenant.
     */
    private void processNewTenant(String tenant, String msg){
        System.out.println("Received message in tenancy subject");
        System.out.println("Message is: " + msg);
        System.out.println("Tenant is: " + tenant);


        JSONObject jsonObj = new JSONObject(msg);
        System.out.println(jsonObj.toString());
        if(jsonObj.isNull("tenant")){
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

    /**
     * Register new callbacks to be invoked when something happens to a subject.
     *
     * The callback should have two parameters: tenant, data
     * @param subject The subject which this subscription is associated to.
     * @param event The event of this subscription.
     * @param callback The callback function. Its signature should be
     * (tenant: str, message:any) : void
     */
    public void on(String subject, String event, BiFunction<String, String, Void> callback){
        System.out.println("Registering a new callback for subject " + subject + " and event " + event);

        if (!this.mEventCallbacks.containsKey(subject)){
            this.mEventCallbacks.put(subject, new HashMap());
        }
        if(!this.mEventCallbacks.get(subject).containsKey(event)){
            this.mEventCallbacks.get(subject).put(event, new ArrayList());
        }
        System.out.println(this.mEventCallbacks.toString());
        this.mEventCallbacks.get(subject).get(event).add(callback);
    }

    /**
     * Given a tenant, bootstrap it to all subjects registered.
     *
     * @param subject The subject being bootstrapped.
     * @param tenant The tenant being bootstrapped.
     * @param mode R/W channel mode (send only, receive only or both).
     * @param isGlobal flag indicating whether this channel should be
     * associated to a service or be global.
     */
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
                try {
                    this.mConsumer.subscribe(retTopic);
                    this.mConsumer.setShouldStop(true);
                } catch (Exception e) {
                    System.out.println(e);
                }
                if(this.mTopics.size() == 1) {
                    System.out.println("Starting Consumer thread...");
                    System.out.flush();
                    try {
                        Thread thread = new Thread(mConsumer);
                        thread.start();
                        System.out.println("... started consumer thread successfully");
                    } catch (Exception error) {
                        System.out.println("Could not start consumer");
                    }
                } else {
                    System.out.println("Consumer thread is already started");
                }
            }

            if (mode.contains("w")) {
                System.out.println("Adding a producer topic: " + subject + " " + tenant + " " + retTopic);
                if (!this.mProducerTopics.containsKey(subject)) {
                    System.out.println("entered here registering new producer");
                    this.mProducerTopics.put(subject, new HashMap());
                }
                this.mProducerTopics.get(subject).put(tenant, retTopic);
            }
        } catch(Exception error) {
            System.out.println("Something went wrong while bootstraping the tenant: " + error);
        }
    }

    /**
     * Creates a new channel tha is related to tenants, subjects, and kafka
     * topics.
     *
     * @param subject The subject associated to this channel.
     * @param mode  Channel type ("r" for only receiving messages, "w" for
     * only sending messages, "rw" for receiving and sending messages).
     * @param isGlobal flag indicating whether this channel should be
     * associated to a service or be global.
     */
    public void createChannel(String subject, String mode, Boolean isGlobal) {
        System.out.println("Creating channel for: " + subject);

        List<String> associatedTenants = new ArrayList<>();
//        List<String> associatedTenants = new ArrayList<>(this.mTenants);
        for(String ten : this.mTenants){
            associatedTenants.add(ten);
        }

        if (isGlobal) {
            associatedTenants.clear();
            associatedTenants.add(Config.getInstance().getInternalTenant());
            this.mGlobalSubjects.put(subject,new HashMap());
            this.mGlobalSubjects.get(subject).put("mode", mode);
        } else {

            System.out.println(this.mTenants);

            System.out.println(associatedTenants);
            this.mSubjects.put(subject,new HashMap());
            this.mSubjects.get(subject).put("mode", mode);
        }
        System.out.println("and tenants: ");
        for (String ten : associatedTenants){
            System.out.println(ten);
            this.bootstrapTenants(subject, ten, mode, isGlobal);
        }

    }

    /**
     * This method is the callback that consumer will call when receives a message.
     *
     * @param topic The topic used to receive the message.
     * @param message The messages received.
     */
    private void processKafkaMessages(String topic, String message){

        this.emit(this.mTopics.get(topic).get("subject"), this.mTopics.get(topic).get("tenant"), "message", message);
    }

    /**
     * Executes all callbacks related to that subject:event.
     *
     * @param subject The subject to be used when emitting this new event.
     * @param tenant The tenant to be used when emitting this new event.
     * @param event  The event to be emitted. This is a arbitrary string.
     * The module itself will emit only ``message`` events (seldomly
     * ``new-tenant`` also)
     * @param data The data to be emitted.
     */
    public void emit(String subject, String tenant, String event, String data){
        System.out.println("Emitting new event " + event + " for subject " + subject + "@" + tenant);

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

    /**
     * Publishes a message in kafka.
     *
     * @param subject The subject to be used when publish the data.
     * @param tenant The tenant associated to that message.
     * @param message The message to be published.
     */
    public void publish(String subject, String tenant, String message){
        if(!this.mProducerTopics.containsKey(subject)){
            System.out.println("No producer was created for this subject: " + subject);
            return;
        }
        if(!this.mProducerTopics.get(subject).containsKey(tenant)){
            System.out.println("No producer was created for this subject: " + subject + "and tenant: " + tenant);
            return;
        }

        this.mProducer.produce(this.mProducerTopics.get(subject).get(tenant), message);
    }

    /**
     * Generate device.create event for active devices on dojot.
     */
    public void generateDeviceCreateEventForActiveDevices(){
        System.out.println("Requested to generate device create events");
        if(this.mTenants.isEmpty()){
            System.out.println("There isn't a tenant created yet.");
            return;
        }

        for (String tenant: this.mTenants){
            this.requestDevice(tenant);
        }
    }

    /**
     * Iterate over devices pagination
     *
     * @param tenant
     */
    public void requestDevice(String tenant){
        Boolean hasNext = true;
        Integer pageNum = 0;
        String extraArg;
        String url;

        while(hasNext){
            extraArg = "";
            if(pageNum > 0){
                extraArg = "?page_num=" + pageNum.toString();
            }
            url = Config.getInstance().getDeviceManagerAddress() + "/device" + extraArg;
            System.out.println("URL:::: " + url);
            hasNext = false;
            try {
                HttpResponse<JsonNode> response = Unirest.get(url)
                        .header("authorization", "Bearer " + Auth.getInstance().getToken(tenant))
                        .asJson();

                JSONObject responseObj = response.getBody().getObject();

                if(responseObj.getJSONObject("pagination").get("has_next").toString().equals("true")){
                    hasNext = true;
                    pageNum = (Integer) responseObj.getJSONObject("pagination").get("next_page");
                }

                JSONArray devices = responseObj.getJSONArray("devices");

                for (int i = 0;i < devices.length();i++){
                    JSONObject device = devices.getJSONObject(i);
                    JSONObject dataEvent = new JSONObject();
                    dataEvent.put("event", "create");
                    dataEvent.put("data", device);
                    this.emit(Config.getInstance().getDeviceManagerDefaultSubject(),tenant,"message", dataEvent.toString());
                }

            } catch (UnirestException exception) {
                System.out.println("Cannot get url: " + url);
                System.out.println("Exception: " + exception.toString());
            }
        }
    }

}
