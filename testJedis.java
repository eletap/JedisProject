import java.io.*;
import java.text.*;
import java.util.*;
import redis.clients.jedis.Jedis;

public class testJedis {

  public static void main(String[] args)  {
	  Jedis jedis = new Jedis("localhost", 6379);
	  jedis.set("damianos", "chatziantoniou");
	  System.out.println(jedis.get("damianos"));
  } // of main

} // of program
