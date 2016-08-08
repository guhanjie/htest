/** 
 * Project Name:		htest 
 * Package Name:	 
 * File Name:			Main.java 
 * Create Date:		2016年6月22日 上午10:15:12 
 * Copyright (c) 2008-2016, guhanjie All Rights Reserved.
 */
/**
 * Class Name:		Main<br/>
 * Description:		[description]
 * @time				2016年6月22日 上午10:15:12
 * @author			guhanjie
 * @version			1.0.0 
 * @since 			JDK 1.6 
 */
public class Main {
	
	private static final String usage = "Welcome to HBase Stress Test Tool, :)\n"
					+ "Usage:  java -jar HBaseTest.jar <arg>, which <arg> list as follows:\n"
					+ "  <help> - print this message and exit.\n"
					+ "  <create> - to start creating table in hbase.\n"
					+ "  <write-direct> - to start writing data directly into hbase.\n"
					+ "  <write-direct-batch> - to start writing data directly and batch into hbase.\n"
					+ "  <write-link> - to start writing data into hbase by Link-HBase.\n"
					+ "  <read-direct> - to start reading data directly from hbase.\n"
					+ "  <read-link> - to start reading data from hbase by Link-HBase.\n"
					+ "  <scan> - to start scaning from hbase(To be done..).";
	
	/**
	 * Method Name:	main<br/>
	 * Description:			[description]
	 * @author				guhanjie
	 * @time					2016年6月22日 上午10:15:12
	 * @param args 
	 */
	public static void main(String[] args) {
    	if(args.length == 0 || "help".equalsIgnoreCase(args[0])) {
    		System.out.println(usage);
    		System.exit(0);
    	}
    	if("create".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [CREATE] module.");
    		HBaseCreate.main(new String[] {""});
    	}
    	if("write-direct".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [WRITE] module.");
    		HBaseInsert.main(new String[] {""});
    	}
    	if("write-direct-batch".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [WRITE] module.");
    		HBaseInsertBatch.main(new String[] {""});
    	}
    	if("write-link".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [WRITE] module.");
    		LinkHBaseInsert.main(new String[] {""});
    	}
    	if("read-direct".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [READ] module.");
    		HBaseRead.main(new String[] {""});
    	}
    	if("read-link".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [READ] module.");
    		LinkHBaseRead.main(new String[] {""});
    	}
    	if("scan".equalsIgnoreCase(args[0])) {
    		System.out.println("Entered into [SCAN] module.");
    		HBaseScan.main(new String[] {""});
    	}
	}
}
