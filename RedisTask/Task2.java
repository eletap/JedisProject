package RedisTask;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.tools.*;

/**
 *The class Task2 reads a query's specification file and then writes, compiles and runs a new java program (JedisQuery.java) into the current folder.
 *JedisQuery will then print out the requested results, separated by ",".
 */

public class Task2{

	public static void main(String[] args)throws IOException{

		try {
			//Read the query's specification file.
			Scanner input = new Scanner(System.in);
			System.out.print("Enter the file name with extension: ");
			String file_name = new String(input.nextLine());
			file_name="../examples/"+file_name;
			input.close();
			FileWriter fw;

			try{
				ReadFile file = new ReadFile(file_name);
				String[] aryLines = file.OpenFile();

				//Start writing the new java program.
				fw = new FileWriter(new File("JedisQuery.java"));
				fw.write("package RedisTask;\n"
						+ "import java.util.*;\n"
						+ "import redis.clients.jedis.Jedis;\n"
						+ "\n");
				fw.write("public class JedisQuery{\n\n"
						+ "	public static void main(String[] args) {\n\n"
						+ "		Jedis jedis = new Jedis(\"localhost\", 6379);\n\n"
						+ "		boolean flag=false;\n");

				aryLines[1]=aryLines[1].replace(" ", "");
				String[] s1 = aryLines[1].split(",");

				for (int i=0; i<s1.length; i++){
					fw.write("		for (String key"+i+":jedis.keys(\"*"+s1[i]+":*\")){"
							+ "\n");

					//The following loop is here just for the appropriate format of the code-blocks.
					for(int j=i; j>=0; j--){
						fw.write("\t");
					}
				}

				fw.write("		if ("+conditionFormat(aryLines[2], s1)+"){\n");

				aryLines[0]=aryLines[0].replace(" ", "");
				String[] s2 = aryLines[0].split(",");

				/**
				 *jedisq represents the first line of the query file (1st line: SELECT).
				 *It modifies each separate value (e.g. Student.FName) so that it follows the syntax rules of jedis(e.g. jedis.hget(Student:1, "FName"))
				 *Its final form represents the exact way in which the results will be printed (all of the results separated by ",").
			   */

				String jedisq = "";
				for (int j=0; j<s2.length; j++){
					if (j!=0) jedisq+="+\", \"+";
					String[] s3 = s2[j].split("\\.");
					jedisq+="jedis.hget(key"+findKey(s1, s3[0])+", \""+s3[1]+"\")";

				}

				for (int i=0; i<=s1.length+1; i++){
					fw.write("\t");
				}
				fw.write("		System.out.println("+jedisq+");\n");
				for (int i=0; i<=s1.length+1; i++){
					fw.write("\t");
				}
				fw.write("		flag=true;\n");

				//Close all open blocks with the appropriate format.
				for(int j=s1.length+1; j>0; j--){
					for (int i=0; i<=j; i++){
						fw.write("\t");
					}
					fw.write("}\n");
				}

				fw.write("		if (flag==false){\n\t"
								+"			System.out.println(\"One of the given keys does not exist. Check the query file.\");\n"
								+"		}\n");
				fw.write("\t}\n");
				fw.write("}\n");
				fw.close();

				compileSrcFile("JedisQuery");

			}catch(IOException e){
				System.out.println( e.getMessage() );
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}//end of main method

	/**
	 *The following method takes the 3rd line of the file(WHERE) and the names of the 'tables' (2nd line of the file(FROM)) as inputs.
	 *Using regular expressions, we look for a string that is going to match the REGEX, e.g. Student.SSN=Grade.SSN.
	 *Within this method each string that is found is modified so that it matches the jedis and java syntax.
	 *e.g. "Student.SSN=Grade.SSN" will be "jedis.hget(key0, "SSN").compareTo(jedis.hget(key1, "SSN"))==0".
	 *Its return value is the final condition that JedisQuery is going to use.
	 */
	public static String conditionFormat(String condition, String[] tables){
		String cond = condition ;
		List<String> alchars = new ArrayList<String>();
		alchars.add("=");
		alchars.add("<");
		alchars.add(">");
		alchars.add(">");
		alchars.add("<>");

		for(String c:alchars){
			String REGEX="(((\\w{1,}[.]\\w{1,})|('([^']*)'))"+c+"((\\w{1,}[.]\\w{1,})|('([^']*)')))";
			Pattern p = Pattern.compile(REGEX);
			Matcher m = p.matcher(condition);
			while(m.find()){
				cond=cond.replace(m.group(1), comparisons(m.group(1), c, tables));
			}
		}

		cond=cond.replace("AND", "&&");
		cond=cond.replace("OR", "||");
		cond=cond.replace("'", "\"");
		return(cond);
	}

	/**
	 *The following method takes the string matched by conditionFormat (e.g. 'Student.SSN=Grade.SSN'), a string-c (=, <,> or <>) and the "tables"
	 *(2nd line of the file) as inputs.
	 *Using regular expressions, we look for a string that is going to match the REGEX, which in this case will look like: "Student.SSN"
	 *Each matched string is modified so that "Student.SSN" will be jedis.hget(key0, "SSN") (where key0 is a redis key to a hash like Student:1).
	 *Each string (c) is replaced by .compareTo (so that we can compare the strings that jedis will return).
	 *Finally, an additional check is added based on the value of the string c, e.g. if c="=" then the compareTo function should return 0 etc.
	 */
	public static String comparisons(String instr, String c, String[] tables){

		String outstr=instr;
		String REGEX="(\\w{1,}[.]\\w{1,})";
		Pattern p = Pattern.compile(REGEX);
		Matcher m = p.matcher(outstr);

		while(m.find()){
			String[] keys=m.group(1).split("\\.");
			outstr=outstr.replace(m.group(1), "jedis.hget(key" +findKey(tables, keys[0])+ ", \""+keys[1]+"\")");
		}

		outstr=outstr.replace(c, ".compareTo(");
		if (c.equals("=")){
			outstr+=")==0";
		}else if (c.equals(">")){
			outstr+=")>0";
		}else if (c.equals("<")){
			outstr+=")<0";
		}else if (c.equals("<>")){
			outstr+=")!=0";
		}

		return outstr;
	}//end of comparisons

	/**
	 *The following method basically looks for a certain String value in an array of Strings.
	 *It will retun the index of the array where the string is found, or -1 if the string does not exist.
	 *It is used so that the final program(JedisQuery) can print the value of the correct key.
	 *It takes an array of all the "tables" (2nd line from input file) and the name of the appropriate key(hash).
	 *e.g. if we are looking for the first student's Student.SSN (or jedis.hget(Student:1, "SSN")), we have to know
	 *the number of the nested 'for' loop (in JedisQuery)which corresponds with "Student:1".
	 */
	public static int findKey(String[] strarray, String substr){
		int rv=-1;
		for (int i=0; i<strarray.length; i++){
			if (strarray[i].equals(substr)){
				 rv = i;
			}
		}
		return rv;
	}//end of findKey

	/**
	 *Compiles a java Program.
	 */
	public static void compileSrcFile(String sourceFileName){
		String fileToCompile = sourceFileName+".java";
		/**
		 *The following line is necessary, because the ToolProvider will only be able to provide a compiler if we run the program with the JDK.
		 *Otherwise (if we run it with JRE) the compiler will return null.
		 */
		System.setProperty("java.home", "C:\\Program Files\\Java\\jdk1.7.0_17");

		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		int compilationResult = compiler.run(null, null, null, fileToCompile);
		if(compilationResult == 0){
			runSrcFile(sourceFileName);
		}else{
			System.out.println("Compilation of"+sourceFileName+".java Failed");
		}
			return;
	}//end of compileSrcFile

	/**
	 *Runs the java Program.
	 */
	public static void runSrcFile(String sourceFileName){
		try {
					Process processCompile = Runtime.getRuntime().exec("javac -cp ..;../lib/jedis-2.5.1.jar"+sourceFileName);
			} catch (IOException e) {
					e.printStackTrace();
			}
			Process processRun = null;
			try {
					processRun = Runtime.getRuntime().exec("java  -cp ..;../lib/jedis-2.5.1.jar RedisTask."+sourceFileName);
			} catch (IOException e) {
					e.printStackTrace();
			}
			try {
					printLines("stdout:", processRun.getInputStream());
					printLines("stderr:", processRun.getErrorStream());
			} catch (Exception e) {
					e.printStackTrace();
			}
	}//end of runSrcFile

	private static void printLines(String name, InputStream ins) throws Exception {
          String line = null;
          BufferedReader in = new BufferedReader(new InputStreamReader(ins));
          while ((line = in.readLine()) != null) {
              System.out.println(name + " " + line);
          }
  }//end of printLines

}//end of RedisProject class
