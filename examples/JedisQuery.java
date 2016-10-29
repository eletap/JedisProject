package RedisTask;
import java.util.*;
import redis.clients.jedis.Jedis;

public class JedisQuery{

	public static void main(String[] args) {

		Jedis jedis = new Jedis("localhost", 6379);

		boolean flag=false;
		for (String key0:jedis.keys("*Student:*")){
			for (String key1:jedis.keys("*Grade:*")){
				if (jedis.hget(key0, "SSN").compareTo(jedis.hget(key1, "SSN"))==0 && jedis.hget(key1, "Mark").compareTo("7")!=0){
						System.out.println(jedis.hget(key0, "FName")+", "+jedis.hget(key0, "LName")+", "+jedis.hget(key1, "Mark")+", "+jedis.hget(key0, "Age"));
						flag=true;
				}
			}
		}
		if (flag==false){
				System.out.println("One of the given keys does not exist. Check the query file.");
		}
	}
}
