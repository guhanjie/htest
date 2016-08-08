

import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScan {

	private static String tableName;
	private static int clientCount;
	private static int clientThreads;
	private static long scanCountPerClient;
	private static String columnFamily = "cf";
	private static String startRow;
	private static String stopRow;
	private static int warningThreshold = 100;
	private static int sendInteval = -1;
	
	private static final String USAGE = "Link HBase read performace test.\n"
					+ " Usage:  <arg1> <arg2> <arg3> <arg4> [<arg5>...].\n"
					+ " Or quit(Ctrl-C) then input : java -cp HBaseTest.jar LinkHBaseRead <arg1> <arg2> <arg3> <arg4> [<arg5>...].\n"
					+ "  <arg1> - the table name to scan\n"
					+ "  <arg2> - the clients count to run\n"
					+ "  <arg3> - the threads count to run in each client\n"
					+ "  <arg4> - the scan counts in each client\n"
					+ "  <arg5>(optional) - the column family to scan, default cf\n"
					+ "  <arg6>(optional) - the start row to scan, defalt not set\n"
					+ "  <arg7>(optional) - the stop row to scan, default not set\n"
					+ "  <arg8>(optional) - the warning threshold for response time(ms), default 100ms\n"
					+ "  <arg9>(optional) - the interval time of running query in each client and thread, defualt no-interval.";
	
    public static void main(String[] args) {
    	scanInput(args);

		final AtomicLong successCount = new AtomicLong(0);
		final AtomicLong warningCount = new AtomicLong(0);
		final AtomicLong errorCount = new AtomicLong(0);
		
        final Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.1.98,192.168.1.99,192.168.1.100");

        final CountDownLatch startLatch = new CountDownLatch(1);

        for (int clientNum = 0; clientNum < clientCount; clientNum++) {
			Thread thread = new Thread("client=" + clientNum) {
				private final AtomicLong oneScanSum = new AtomicLong(scanCountPerClient);
				private final Executor executor = Executors.newFixedThreadPool(clientThreads);
				private final HTable hTable;
				{
		            HTable hbaseTable = null;
		            try {
		                hbaseTable = new HTable(configuration, tableName);
		            } catch (Exception e2) {
		                e2.printStackTrace();
		                System.exit(1);
		            }
		            hTable = hbaseTable;
		            System.out.println("htable for " + tableName + " set!!");
				}
				public void run() {
					try {
						startLatch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < clientThreads; i++) {
						executor.execute(new Runnable() {
							public void run() {
		                        while (oneScanSum.decrementAndGet() >= 0) {
		                        	Scan scan = new Scan();
		                        	if(columnFamily != null && columnFamily.length() > 0) {
		                        		scan.addFamily(columnFamily.getBytes());
		                        	}
		                        	if(startRow != null && startRow.length() > 0) {
		                        		scan.setStartRow(startRow.getBytes());
		                        	}
		                        	if(stopRow != null && stopRow.length() > 0) {
		                        		scan.setStopRow(stopRow.getBytes());
		                        	}
		                            try {
										long begin = System.currentTimeMillis();
										ResultScanner rs = hTable.getScanner(scan);
										Iterator<Result> iterator = rs.iterator();
										while(iterator.hasNext()) {
											iterator.next();
											if ((System.currentTimeMillis() - begin) > warningThreshold) {
												warningCount.incrementAndGet();
											}
			                                successCount.incrementAndGet();
			                                begin = System.currentTimeMillis();
										}
		                            } catch (Exception e) {
		                            	errorCount.incrementAndGet();
		                                e.printStackTrace();
		                            }
									if (sendInteval > 0) {
										try {
											Thread.sleep(sendInteval);
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
									}
		                        }
							}
						});
					}
				}
			};
			thread.setDaemon(true);
			thread.start();
        }

        startLatch.countDown();
        
		long start = System.currentTimeMillis();
		while ((successCount.get() + errorCount.get()) < (clientCount * scanCountPerClient)) {
			long src = successCount.get();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			long now = successCount.get();
			long cur = System.currentTimeMillis();
			System.out.println("ClientCount: ["+ clientCount + "], ThreadNum/perClient: ["+clientThreads + "], "
					+ "per second: [" + (now - src) + "], qps: [" + (int) (1000.0 * now / (cur - start)) + "]"
					+ "\nsuccesscount=" + now + ", warningcount=" + warningCount.get() + ", errorCount=" + errorCount.get());
		}
	}

    public static void scanInput(String[] args) {
    	//command mode
    	try {
        	if(args.length >= 4) {	
        		tableName = args[0];
        		clientCount = Integer.parseInt(args[1]);
        		clientThreads = Integer.parseInt(args[2]);
        		scanCountPerClient = Long.parseLong(args[3]);
        		columnFamily = args.length>4 ? args[4] : "cf";
        		startRow = args.length>5 ? args[5] : "";
        		stopRow = args.length>6 ? args[6] : "";
        		warningThreshold = args.length>6 ? Integer.parseInt(args[6]) : 100;
        		sendInteval = args.length>7 ? Integer.parseInt(args[7]) : -1;
        		return;
        	}
    	} catch(Exception e) {
    		System.err.println("Command args resolved error.");
    		System.exit(1);
    	}
    	//interactive mode
    	Scanner scanner = new Scanner(System.in);
    	try {
        	while(true) {
        		System.out.println(USAGE);
	    		String str = scanner.nextLine();
	    		Pattern p = Pattern.compile("(\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(\\S+)?\\s+(\\S+)?\\s+(\\S+)?\\s+(\\d+)?\\s+(\\d+)?", Pattern.DOTALL);
	    		Matcher m = p.matcher(str);
	    		if(!m.matches()) {
	    			System.err.println("Syntax error. Please input as following:");
	    			continue;
	    		}
	        	tableName = m.group(1);
	        	clientCount = Integer.parseInt(m.group(2));
	        	clientThreads = Integer.parseInt(m.group(3));
	        	scanCountPerClient = Long.parseLong(m.group(4));
	        	columnFamily = (m.group(5)!=null) ? m.group(5) : "";
	        	startRow = (m.group(6)!=null) ? m.group(6) : "";
	        	stopRow = (m.group(7)!=null) ? m.group(7) : "";
	    		warningThreshold = (m.group(8)!=null) ? Integer.parseInt(m.group(8)) : 100;
	    		sendInteval = (m.group(9)!=null) ? Integer.parseInt(m.group(9)) : -1;
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
