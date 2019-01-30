package com.cpqd.app.sample;

import com.cpqd.app.config.Config;
import com.cpqd.app.messenger.Messenger;

import java.util.concurrent.TimeUnit;

public class Sample {
    public static void main (String[] args){
        Messenger msg = new Messenger();
        msg.init();

        msg.createChannel("device-data", "rw", false);
        msg.on("device-data", "message", (a,b) -> {System.out.println(a + b); return null;});
        msg.createChannel(Config.getInstance().getDeviceManagerDefaultSubject(), "r", false);
        msg.on(Config.getInstance().getDeviceManagerDefaultSubject(), "message", (a,b) -> {System.out.println(a + b); return null;});

        try {
            TimeUnit.SECONDS.sleep(15);
        } catch (InterruptedException e){
            System.out.println(e);
        }

        //Testing if another tenant than admin receives message
        msg.publish("device-data", "math5", "{\"aaa\": \"aaaa\"}");
        try {
            TimeUnit.SECONDS.sleep(120);
        } catch (InterruptedException e){
            System.out.println(e);
        }
        
        //Testing if tenant created during the lib is working is receiving messages
        msg.publish("device-data", "math6", "{\"bbb\": \"bbb\"}");
    }
}
