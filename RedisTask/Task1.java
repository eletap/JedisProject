package RedisTask;
import java.io.IOException;
import java.util.*;
import redis.clients.jedis.Jedis;

/**
 *The class Task1 reads a relation's specification file and then inserts relation's data into Redis in an appropriate format.
 */
public class Task1{

	public static void main(String[] args)throws IOException{

		try {

			Scanner input = new Scanner(System.in);
			System.out.print("Enter the file name with extention : ");
			String file_name = new String(input.nextLine());
			file_name="../examples/"+file_name;

			//input.close();

			Jedis jedis = new Jedis("localhost", 6379);

			try{
				ReadFile file = new ReadFile(file_name);
				String[] aryLines = file.OpenFile();

				int i = 0;
				int k=0;
				List<String> columns = new ArrayList<String>();

				/**
				 *Add each 'column' (e.g. FName, LName etc) to the columns list and keeps record of the total number of 'columns'.
				 */
				while( (i<aryLines.length) && (!aryLines[i].contains(";")) ){
						columns.add(aryLines[i]);
						i++;
						k++;
				}

				/**
				 *Split(;) the values for each line after the line ";"
				 */
				for(int g=k+1; g < aryLines.length; g++ ){

						String[] inserts = aryLines[g].split(";");

						/**
						 *Insert the values to the appropriate keys.
						 *Data type used:hash, e.g. Student:1 SSN 12938
						 */
						for(int z = 1; z < columns.size(); z++) {
							jedis.hset(columns.get(0)+":" + (g-k), columns.get(z),inserts[z-1]);
						}
				}
			}catch(IOException e){
				System.out.println( e.getMessage() );
			}//end of try-catch2

		} catch (Exception ex) {
			ex.printStackTrace();
		}//end of try-catch1

	}//end of main method

}//end of RedisProject class
