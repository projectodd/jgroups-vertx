package org.projectodd.vertx.jgroups.integration;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.projectodd.vertx.jgroups.ChannelFactory;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class JGroupsTestVerticle extends Verticle implements Receiver {

    private Channel channel;
    private String uuid;
    private View view;

    @Override
    public void start(Future<Void> startedResult) {
        final String cluster = container.config().getString( "cluster" );
        
        this.uuid = UUID.randomUUID().toString();
        ChannelFactory factory = new ChannelFactory(this.container, this.vertx, cluster, this.uuid );
        try {
            channel = factory.newChannel();
            channel.setReceiver( this );
            channel.connect(cluster);
            
            this.vertx.eventBus().registerHandler("test." + cluster, new Handler<org.vertx.java.core.eventbus.Message<Boolean>>() {
                @Override
                public void handle(org.vertx.java.core.eventbus.Message<Boolean> event) {
                    JsonArray response = new JsonArray();
                    for ( Address each : view.getMembers() ) {
                        response.add( each.toString() );
                    }
                    vertx.eventBus().send("test." + cluster + ".responses", response );
                }
            } );
           
            startedResult.setResult(null);
        } catch (Exception e) {
            e.printStackTrace();
            startedResult.setFailure(e);
        }
    }

    @Override
    public void receive(Message msg) {
        System.err.println( "received: " + msg );
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        
    }

    @Override
    public void setState(InputStream input) throws Exception {
        
    }

    @Override
    public void viewAccepted(View view) {
        this.view = view;
    }

    @Override
    public void suspect(Address suspected_mbr) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void block() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void unblock() {
        // TODO Auto-generated method stub
        
    }

}
