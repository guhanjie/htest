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

public class HBaseInsert {
	
	private static String tableName;
	private static String columnFamily;
	private static String columnQualifier;
	private static int clientCount;
	private static int clientThreads;
	private static long startKey = 0L;
	private static long endKey = Long.MAX_VALUE;
	private static int warningThreshold = 100;
	
	private static final String USAGE = "HBase write directly performace test.\n"
					+ " Usage:  <arg1> <arg2> <arg3> <arg4> <arg5> [<arg6>...].\n"
					+ " Or quit(Ctrl-C) then input : java -cp HBaseTest.jar HBaseInsert <arg1> <arg2> <arg3> <arg4> <arg5> [<arg6>...].\n"
					+ "  <arg1> - the table name to write\n"
					+ "  <arg2> - the column family to write\n"
					+ "  <arg3> - the column qualifier to write\n"
					+ "  <arg4> - the clients count to run\n"
					+ "  <arg5> - the threads count to run in each client\n"
					+ "  <arg6>(optional) - the start key to write, defalt 0\n"
					+ "  <arg7>(optional) - the end key to write, default max\n"
					+ "  <arg8>(optional) - the warning threshold for response time(ms), default 100ms.";
		
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
							private final Random random = new Random();
							public void run() {
		                        while (true) {
		                        	long rowkey = (random.nextLong() & Long.MAX_VALUE) % (endKey-startKey) + startKey;
	                                Put put = new Put(String.valueOf(rowkey).getBytes());
	                                put.addColumn(columnFamily.getBytes(), columnQualifier.getBytes(), new String("balabala~"+random.nextLong()).getBytes());
		                            try {
										long begin = System.currentTimeMillis();
		                                hTable.put(put);
										if ((System.currentTimeMillis() - begin) > warningThreshold) {
											warningCount.incrementAndGet();
										}
		                                successCount.incrementAndGet();
		                            } catch (Exception e) {
		                            	errorCount.incrementAndGet();
		                                e.printStackTrace();
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
        while ((successCount.get() + errorCount.get()) < (endKey-startKey)) {
            long success1 = successCount.get();
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long success2 = successCount.get();
			long cur = System.currentTimeMillis();
			System.out.println("ClientCount: ["+ clientCount + "], ThreadNum/perClient: ["+clientThreads + "], "
							+ "per second: [" + (success2 - success1) + "], tps: [" + (int) (1000.0 * success2 / (cur - start)) + "]"
							+ "\nsuccesscount=" + success2 + ", warningcount=" + warningCount.get() + ", errorCount=" + errorCount.get());
        }
    }
    
    public static void scanInput(String[] args) {
    	//command mode
    	try {
	    	if(args.length >= 5) {
	    		tableName = args[0];
	    		columnFamily = args[1];
	    		columnQualifier = args[2];
	    		clientCount = Integer.parseInt(args[3]);
	    		clientThreads = Integer.parseInt(args[4]);
	    		startKey = args.length>5 ? Long.parseLong(args[5]) : 0L;
	    		endKey = args.length>6 ? Long.parseLong(args[6]) : Long.MAX_VALUE;
	    		warningThreshold = args.length>7 ? Integer.parseInt(args[7]) : 100;
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
	    		Pattern p = Pattern.compile("(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)?\\s+(\\d+)?\\s+(\\d+)?", Pattern.DOTALL);
	    		Matcher m = p.matcher(str);
	    		if(!m.matches()) {
	    			System.err.println("Syntax error. Please input as following:");
	    			continue;
	    		}
	        	tableName = m.group(1);
	        	columnFamily = m.group(2);
	        	columnQualifier = m.group(3);
	        	clientCount = Integer.parseInt(m.group(4));
	        	clientThreads = Integer.parseInt(m.group(5));
	        	startKey = (m.group(6)!=null) ? Long.parseLong(m.group(6)) : 0L;
	        	endKey = (m.group(7)!=null) ? Long.parseLong(m.group(7)) : Long.MAX_VALUE;
	    		warningThreshold = (m.group(8)!=null) ? Integer.parseInt(m.group(8)) : 100;
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