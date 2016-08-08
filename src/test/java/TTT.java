import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 
 * Project Name:		htest 
 * Package Name:	 
 * File Name:			TTT.java 
 * Create Date:		2016年6月21日 上午10:30:56 
 * Copyright (c) 2008-2016, guhanjie All Rights Reserved.
 */
/**
 * Class Name:		TTT<br/>
 * Description:		[description]
 * @time				2016年6月21日 上午10:30:56
 * @author			guhanjie
 * @version			1.0.0 
 * @since 			JDK 1.6 
 */
public class TTT {
	
	/**
	 * Method Name:	main<br/>
	 * Description:			[description]
	 * @author				guhanjie
	 * @time					2016年6月21日 上午10:30:56
	 * @param args 
	 */
	public static void main(String[] args) {
		System.out.println("test success.");
		scanInput();
	}

	private static String tableName;
	private static int clientCount;
	private static int clientThreads;
	private static long queryCountPerClient;
	private static long startKey = 0L;
	private static long endKey = Long.MAX_VALUE;
	private static int warningThreshold = 100;
	private static int sendInteval = -1;

	private static final String USAGE = "HBase write directly performace test.\n"
					+ " Usage:  <arg1> <arg2> <arg3> <arg4> <arg5> [<arg6>...].\n"
					+ "  <arg1> - the table name to write\n"
					+ "  <arg2> - the column family to write\n"
					+ "  <arg3> - the column qualifier to write\n"
					+ "  <arg4> - the clients count to run\n"
					+ "  <arg5> - the threads count to run in each client\n"
					+ "  <arg6>(optional) - the batch count per write, default 1\n"
					+ "  <arg7>(optional) - the start key to write, defalt 0\n"
					+ "  <arg8>(optional) - the end key to write, default max\n"
					+ "  <arg9>(optional) - the warning threshold for response time(ms), default 100ms.";
		
    public static void scanInput() {
    	Scanner scanner = new Scanner(System.in);
    	try {
        	while(true) {
        		System.out.println(USAGE);
	    		String str = scanner.nextLine();
	    		Pattern p = Pattern.compile("(\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)?\\s+(\\d+)?\\s+(\\d+)?\\s+(\\d+)?", Pattern.DOTALL);
	    		Matcher m = p.matcher(str);
	    		if(!m.matches()) {
	    			System.err.println("Syntax error. Please input as following:");
	    			continue;
	    		}
	        	tableName = m.group(1);
	        	clientCount = Integer.parseInt(m.group(2));
	        	clientThreads = Integer.parseInt(m.group(3));
	        	queryCountPerClient = Long.parseLong(m.group(4));
	        	startKey = (m.group(5)!=null) ? Long.parseLong(m.group(5)) : 0L;
	        	endKey = (m.group(6)!=null) ? Long.parseLong(m.group(6)) : Long.MAX_VALUE;
	    		warningThreshold = (m.group(7)!=null) ? Integer.parseInt(m.group(7)) : 100;
	    		sendInteval = (m.group(8)!=null) ? Integer.parseInt(m.group(8)) : -1;

		        System.out.println("tableName: \t"+tableName);
		        System.out.println("clientCount: \t"+clientCount);
		        System.out.println("clientThreads: \t"+clientThreads);
		        System.out.println("batch: \t"+queryCountPerClient);
		        System.out.println("startKey: \t"+startKey);
		        System.out.println("endKey: \t"+endKey);
		        System.out.println("warningThreshold: \t"+warningThreshold);
		        System.out.println("sendInteval: \t"+sendInteval);
	    		return;
        	}
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	} finally {
    		scanner.close();
    	}
    }
}
