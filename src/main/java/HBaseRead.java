import java.io.IOException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HBaseRead {

	private static String tableName;
	private static int clientCount;
	private static int clientThreads;
	private static long queryCountPerClient;
	private static long startKey = 0L;
	private static long endKey = Long.MAX_VALUE;
	private static int warningThreshold = 100;
	private static int sendInteval = -1;
	
	private static final String USAGE = "HBase read directly performace test.\n"
					+ " Usage:  <arg1> <arg2> <arg3> <arg4> [<arg5>...].\n"
					+ " Or quit(Ctrl-C) then input : java -cp HBaseTest.jar LinkHBaseRead <arg1> <arg2> <arg3> <arg4> [<arg5>...].\n"
					+ "  <arg1> - the table name to query\n"
					+ "  <arg2> - the clients count to run\n"
					+ "  <arg3> - the threads count to run in each client\n"
					+ "  <arg4> - the query counts in each client\n"
					+ "  <arg5>(optional) - the start key to query, defalt 0\n"
					+ "  <arg6>(optional) - the end key to query, default random in runtime\n"
					+ "  <arg7>(optional) - the warning threshold for response time(ms), default 100ms\n"
					+ "  <arg8>(optional) - the interval time of running query in each client and thread, defualt no-interval.";
		
    public static void main(String[] args) {
    	scanInput(args);

        final AtomicLong successCount = new AtomicLong(0L);
		final AtomicLong warningCount = new AtomicLong(0);
        final AtomicLong errorCount = new AtomicLong(0L);

        final Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.1.98,192.168.1.99,192.168.1.100");

        final CountDownLatch startLatch = new CountDownLatch(1);

        for (int clientNum = 0; clientNum < clientCount; clientNum++) {
			Thread thread = new Thread("client=" + clientNum) {
				private final AtomicLong oneSenderMsgSum = new AtomicLong(queryCountPerClient);
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
		            System.out.println("htable for " + tableName + " get!!");
				}
				public void run() {
					try {
						startLatch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < clientThreads; i++) {
						executor.execute(new Runnable() {
							private final Random random = new Random();
							public void run() {
		                        while (oneSenderMsgSum.decrementAndGet() >= 0) {
		                        	long begin = System.currentTimeMillis();
									long queryKey = (endKey == startKey) ? startKey : (random.nextLong() & Long.MAX_VALUE) % (endKey-startKey) + startKey;
									if(sendInteval > 500) {
										System.out.println("query rowKey: "+queryKey);
									}
	                                Get get = new Get(String.valueOf(queryKey).getBytes());
	                                try {
	                                    Result result = hTable.get(get);
//										if (!result.isEmpty()) {
											if ((System.currentTimeMillis() - begin) > warningThreshold) {
												warningCount.incrementAndGet();
											}
											successCount.incrementAndGet();											
//										} else {
//											errorCount.incrementAndGet();
//										}
	                                } catch (Exception e) {
	                                	errorCount.incrementAndGet();
	                                    e.printStackTrace();
	                                }
		                        }
		                        try {
		                            hTable.close();
		                        } catch (IOException e) {
		                            e.printStackTrace();
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
		while ((successCount.get() + errorCount.get()) < (clientCount * queryCountPerClient)) {
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
        		queryCountPerClient = Long.parseLong(args[3]);
        		startKey = args.length>4 ? Long.parseLong(args[4]) : 0L;
        		endKey = args.length>5 ? Long.parseLong(args[5]) : Long.MAX_VALUE;
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
