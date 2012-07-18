package com.zhihu.sink;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: shenchen
 * Date: 7/17/12
 * Time: 7:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestClient {
    private Client subClient;
    private Client pubClient;
    private static ExecutorService pool;

    @Before
    public void setUp(){
        subClient = new Client("localhost");
        pubClient = new Client("localhost");
        pool = Executors.newFixedThreadPool(1);
        pool.execute(new Runnable() {
            @Override
            public void run() {
                try{
                    for( int i = 0 ; i < 10 ; i ++){
                        pubClient.publish("aaa","hello"+i);
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void testPubSub() throws InterruptedException {
        PubSub pubsub = subClient.subscribe("aaa");
        for( int i = 0 ; i < 10 ; i ++ ){
            String s = pubsub.next();
            System.out.println(s);
            if(i == 0){
                subClient.disconnect();
                Thread.sleep(1000);
            }
            assertEquals(s,"hello"+i);
        }
    }

    @After
    public void tearDown(){
    }
}
