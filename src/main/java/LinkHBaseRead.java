import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pinganfu.linkhbase.hclient.LinkHbaseAccessService;
import com.pinganfu.linkhbase.model.QueryResult;

/**
 * 
 * @author saitxuc
 *
 */
public class LinkHBaseRead {

	private static String tableName;
	private static int clientCount;
	private static int clientThreads;
	private static long queryCountPerClient;
	private static long startKey = 0L;
	private static long endKey = Long.MAX_VALUE;
	private static int warningThreshold = 100;
	private static int sendInteval = -1;
	
	private static final String USAGE = "Link HBase read performace test.\n"
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
    	
	    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"/provider.xml"});
	    context.start();
        final LinkHbaseAccessService linkhbaseService = (LinkHbaseAccessService)context.getBean("linkHbaseService");

		final AtomicLong successCount = new AtomicLong(0);
		final AtomicLong warningCount = new AtomicLong(0);
		final AtomicLong errorCount = new AtomicLong(0);
		
		final CountDownLatch startLatch = new CountDownLatch(1);
		
		for (int clientNum = 0; clientNum < clientCount; clientNum++) {
			Thread thread = new Thread("client=" + clientNum) {
				private final AtomicLong oneSenderMsgSum = new AtomicLong(queryCountPerClient);
				private final Executor executor = Executors.newFixedThreadPool(clientThreads);
				public void run() {
					try {
						startLatch.await();
						System.out.println("Ready to read table[" + tableName + "] by Link-HBase!!!");
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < clientThreads; i++) {
						executor.execute(new Runnable() {
							private final Random random = new Random();
							public void run() {
								try {
									int seqnum = 0;
									while (oneSenderMsgSum.decrementAndGet() >= 0) {
										long begin = System.currentTimeMillis();
										long queryKey = (endKey == startKey) ? startKey : (random.nextLong() & Long.MAX_VALUE) % (endKey-startKey) + startKey;
										if(sendInteval > 500) {
											System.out.println("query rowKey: "+queryKey);
										}
										QueryResult result = linkhbaseService.find(tableName, String.valueOf(queryKey));
										if (!result.isError()) {	// && result.getReturnList().size() > 0
											if ((System.currentTimeMillis() - begin) > warningThreshold) {
												warningCount.incrementAndGet();
											}
											successCount.incrementAndGet();											
										} else {
											errorCount.incrementAndGet();
										}
										if (sendInteval > 0) {
											try {
												Thread.sleep(sendInteval);
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
										}
										seqnum++;
									}
								} catch(Exception e) {
									errorCount.incrementAndGet();
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
