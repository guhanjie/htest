import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Owen on 2016/4/22.
 */
public class HBaseCreate {
	
	private static String tableName;
	private static int cfCount;
    private static List<String> cfNames = new ArrayList<String>();
    private static boolean isPreSplit = false;
    private static String startKey;
    private static String endKey;
    private static int splitCount;
	
    public static void main(String[] args)
    {
    	scanInput();
        System.out.println("Starting to creat table ["+tableName+"]...");
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.1.98,192.168.1.99,192.168.1.100");
        HBaseAdmin hAdmin = null;
        try
        {
        	hAdmin = new HBaseAdmin(configuration);
            if (hAdmin.tableExists(tableName)) {
                System.out.println("table has existed.");
            } else {
                System.out.println("table not exists.");
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for(int i=0; i<cfCount; i++) {
                    HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes(cfNames.get(i)));
                    columnDesc.setMaxVersions(1);
                    tableDesc.addFamily(columnDesc);
                }
                if(isPreSplit) {
                	createTable(hAdmin, tableDesc, getHexSplits(startKey, endKey, splitCount));
                } else {
                	createTable(hAdmin, tableDesc, null);
                }
                System.out.println("table " + tableName + " has been created!!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	if(hAdmin != null) {        		
        		try {
					hAdmin.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
        	}
        }
    }
    
    public static void scanInput() {
    	//interactive mode
    	Scanner scanner = new Scanner(System.in);
    	try {
	    	System.out.println("Enter the name of table:");
	        tableName = scanner.nextLine();
	        System.out.println("Enter the count of column family:");
	        cfCount = scanner.nextInt();
	        scanner.nextLine();
	        for(int i=1; i<=cfCount; i++) {
	            System.out.println("Enter the name of the ["+i+"]th column family:");
	            cfNames.add(scanner.nextLine());
	        }
	        System.out.println("Are you going to pre-split table? Please enter yes or no?");
	        isPreSplit = scanner.nextLine().equalsIgnoreCase("yes");
	        if(isPreSplit) {
	        	System.out.println("Enter the start key:");
	        	startKey = scanner.nextLine();
	        	System.out.println("Enter the end key:");
	        	endKey = scanner.nextLine();
	        	System.out.println("Enter the split count:");
	        	splitCount = scanner.nextInt();
	        }
	        System.out.println("tableName: \t"+tableName);
	        System.out.println("cfCount: \t" +cfCount);
	        System.out.println("cfNames: \t"+Arrays.deepToString(cfNames.toArray()));
	        System.out.println("isPreSplit: \t"+isPreSplit);
	        System.out.println("startKey: \t"+startKey);
	        System.out.println("endKey: \t"+endKey);
	        System.out.println("splitCount: \t"+splitCount);
    	} catch(Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}finally {
    		scanner.close();
    	}
    }

    public static boolean createTable(HBaseAdmin admin, HTableDescriptor table, byte[][] splits) throws IOException {
        try {
        	if(splits==null){
        		admin.createTable(table);
        	}else{        		
        		admin.createTable(table, splits);
        	}
            return true;
        } catch (TableExistsException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions)
    {
    	if(numRegions<=1){
    		return null;
    	}
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", new Object[] { key }).getBytes();
            splits[i] = b;
        }
        return splits;
    }
}
