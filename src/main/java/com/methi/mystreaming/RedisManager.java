package com.methi.mystreaming;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.Set;

public class RedisManager implements Runnable {


    private static String lbKey = "items-leaderboard";
    private Jedis jedis;
    public void setup(){
        this.jedis = new Jedis("localhost");
        jedis.del(lbKey);
        System.out.println("Redis connection setup successfully");
    }

    @Override
    public void run() {
        System.out.println("Starting Redis Manager");
        //fetching all values in reverse order of scores from the zset of redis
        try {
            while(true) {
                Set<Tuple> scores = jedis.zrevrangeWithScores(lbKey, 0, -1);

                Iterator<Tuple> iScores = scores.iterator();
                int position = 1;

                while (iScores.hasNext()) {
                    Tuple score = iScores.next();
                    System.out.println(
                           "Leaderboard - " + position + " : "
                                    + score.getElement() + " = " + score.getScore());
                    position++;
                }
                //sleep for 3 secs
                Thread.sleep(3000);
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }

    }

    public void close(){
        this.jedis.close();
    }
}
