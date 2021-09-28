package com.methi.mystreaming;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;

public class RedisWriter extends ForeachWriter<Row> {
    private Jedis jedis;
    private static final String lbKey = "items-leaderboard";

    @Override
    public boolean open(long partitionId, long epochId) {
        this.jedis = new Jedis("localhost");
        return true;
    }

    @Override
    public void process(Row value) {
        //incrementing each key by the value provided in the row
        this.jedis.zincrby(lbKey,value.getInt(3),value.getString(1));
    }

    @Override
    public void close(Throwable errorOrNull) {
        this.jedis.close();
    }
}
