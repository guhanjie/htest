import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pinganfu.linkhbase.hclient.LinkHbaseAccessService;


public class LinkHBaseInsert {
	
	private static String tableName;
	private static int clientCount;
	private static int clientThreads;
	private static String columnQualifier = "q1";
	private static long writeCount = Long.MAX_VALUE;
	private static int isderect = 0;
	private static int warningThreshold = 100;
	
	private static final String USAGE = "Link HBase write performace test.\n"
					+ " Usage:  <arg1> <arg2> <arg3> [<arg4>...].\n"
					+ " Or quit(Ctrl-C) then input : java -cp HBaseTest.jar LinkHBaseInsert <arg1> <arg2> <arg3> [<arg4>...].\n"
					+ "  <arg1> - the table name to write\n"
					+ "  <arg2> - the clients count to run\n"
					+ "  <arg3> - the threads count to run in each client\n"
					+ "  <arg4>(optional) - whether or not directly put(without param validation) to Link-HBase, 0:put, 1:directly-put, default 0\n"
					+ "  <arg5>(optional) - the column qualifier to write\n"
					+ "  <arg6>(optional) - the total write count, defalt max\n"
					+ "  <arg7>(optional) - the warning threshold for response time(ms), default 100ms.";
		
    public static void main(String[] args) {
    	scanInput(args);
    	
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"/provider.xml"});
        context.start();
        final LinkHbaseAccessService linkhbaseService = (LinkHbaseAccessService)context.getBean("linkHbaseService"); 
    	
        final AtomicLong successCount = new AtomicLong(0L);
		final AtomicLong warningCount = new AtomicLong(0);
        final AtomicLong errorCount = new AtomicLong(0L);

        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.1.98,192.168.1.99,192.168.1.100");

        final CountDownLatch startLatch = new CountDownLatch(1);

        for (int clientNum = 0; clientNum < clientCount; clientNum++) {
			Thread thread = new Thread("client=" + clientNum) {
				private final Executor executor = Executors.newFixedThreadPool(clientThreads);
				public void run() {
					try {
						startLatch.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for (int i = 0; i < clientThreads; i++) {
						executor.execute(new Runnable() {
							public void run() {
								while (true) {
		                    		Map<String, String> content=new HashMap<String, String>();
		                    		content.put(columnQualifier, new Random().nextLong()+"");
		                            try {
										long begin = System.currentTimeMillis();
		                            	if(isderect==1){
		                            		linkhbaseService.directlyPut(tableName, content);
		                            	}else{
		                            		linkhbaseService.put(tableName, content);
		                            	}
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
        while ((successCount.get() + errorCount.get()) < writeCount) {
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
	    	if(args.length >= 3) {
	    		tableName = args[0];
	    		clientCount = Integer.parseInt(args[1]);
	    		clientThreads = Integer.parseInt(args[2]);
	    		columnQualifier = args.length>3 ? args[3] : "q1";
	    		isderect = args.length>4 ? Integer.parseInt(args[4]) : 0;
	    		writeCount = args.length>5 ? Long.parseLong(args[5]) : 0L;
	    		warningThreshold = args.length>6 ? Integer.parseInt(args[6]) : 100;
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
	    		Pattern p = Pattern.compile("(\\S+)\\s+(\\d+)\\s+(\\d+)\\s+(\\S+)?\\s+(\\d)?\\s+(\\d+)?\\s+(\\d+)?", Pattern.DOTALL);
	    		Matcher m = p.matcher(str);
	    		if(!m.matches()) {
	    			System.err.println("Syntax error. Please input as following:");
	    			continue;
	    		}
	        	tableName = m.group(1);
	        	clientCount = Integer.parseInt(m.group(2));
	        	clientThreads = Integer.parseInt(m.group(3));
	        	columnQualifier = (m.group(4)!=null) ? m.group(4) : "q1";
	        	isderect = (m.group(5)!=null) ? Integer.parseInt(m.group(5)) : 0;
	        	writeCount = (m.group(6)!=null) ? Long.parseLong(m.group(6)) : Long.MAX_VALUE;
	    		warningThreshold = (m.group(7)!=null) ? Integer.parseInt(m.group(7)) : 100;
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