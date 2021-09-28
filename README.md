# Spark Streaming Example
we will try to create a leaderboard items which will tell which generated the greatest revenue.

## Project Setup
We need docker on the machine to run this project or alternatively Redis, zookeeper and kafka installed.

if having docker then run <code>docker-compose up -d</code>. 
This will execute docker-compose.yml file to spawn containers of
zookeeper, kafka and redis.

## Project Description
This is a basic spark streaming project where spark will write the 
kafka stream data into redis to update leaderboard. 
This is an example on how to create a live leaderboard.
Further improvements could have been made if we have used mariaDB in place of redis to make support for 
time range queries. Redis is used because of its highly efficient in-memory fetch performance.


## Code Description
This contains one package <code>com.methi.mystreaming</code> inside which we have our files.

KafkaDataGenerator is a runnable class which generate random items and push it to kafka.

KafkaDataConsumer is a runnable class which can be used to validate KafkaDataGenerator class. It reads from the same 
topic and prints the output on console.

RedisManager class is a runnable which do a zrevrangewithscore query on redis 
to poll the leaderboard data set
from redis.

RedisWriter is a class that extends ForeachWriter. It fetches the two values (name, price) from the row and 
write it into redis by incrementing its value by price.

StreamingSalesDashBoard is main class running all the required threads. It starts 
spark streaming job which read from kafka topic and write into redis using the redisWriter class.



