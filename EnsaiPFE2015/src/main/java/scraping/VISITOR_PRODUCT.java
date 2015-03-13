package scraping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class VISITOR_PRODUCT {

    public static final String CSV_URL = "http://real-chart.finance.yahoo.com/table.csv?s=%s";

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /** Keyspace name */
    public static final String KEYSPACE = "abb";
    /** Table name */
    public static final String TABLE = "visitor_product";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s ("
    		+ "visitor_id varchar, "
    		+ "visit_id varchar, "
			+ "product_id varchar, "
			+ "is_visit_converted int, "
			+ "ts varchar, "
			+ "view_quantity int, "
			+ "basket_quantity int, "
			+ "order_quantity int, "
			+ "stock int, "
			+ "unit_price double, "
			+ "unit_sale_price double, "
			+ "unit_sale_percent double, "
			+ "unit_shipping_price double, "
			+ "shipping_time varchar, "
			+ "PRIMARY KEY ((visitor_id), visit_id, product_id) "
	  + ") WITH "
			+ "bloom_filter_fp_chance=0.010000 AND "
			  + "caching='KEYS_ONLY' AND "
			  + "comment='' AND "
			  + "dclocal_read_repair_chance=0.100000 AND "
			  + "gc_grace_seconds=864000 AND "
			  + "index_interval=128 AND "
			  + "read_repair_chance=0.000000 AND "
			  + "replicate_on_write='true' AND "
			  + "populate_io_cache_on_flush='false' AND "
			  + "default_time_to_live=0 AND "
			  + "speculative_retry='99.0PERCENTILE' AND "
			  + "memtable_flush_period_in_ms=0 AND "
			  + "compaction={'class': 'SizeTieredCompactionStrategy'} AND "
			  + "compression={'sstable_compression': 'LZ4Compressor'};", KEYSPACE, TABLE);

    

    
    
    
    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT = String.format("INSERT INTO  abb.visitor_product "
    		+ "(visitor_id,visit_id,product_id,is_visit_converted,ts,view_quantity,basket_quantity,order_quantity,stock,unit_price,unit_sale_price,unit_sale_percent,unit_shipping_price,shipping_time) "
    		+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", KEYSPACE, TABLE);

    public  void run(String[] args){
    	
        if (args.length == 0){
            System.out.println("usage: java bulkload.BulkLoad <list of ticker symbols>");
            return;
        }

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        generateSSTABLE1(outputDir,args);
        //generateSSTABLE2(outputDir,args);
        
    }
    
    
    

    private void generateSSTABLE1(File outputDir, String[] args){
		CsvListReader csvReader;
    	BufferedReader reader;
    	CQLSSTableWriter.Builder builder;
    	int lineCounter, totalRecord = 0;
    	List<String> line;
    	
    	// Prepare SSTable writer
    	builder = CQLSSTableWriter.builder();
    	// set output directory
    	builder.inDirectory(outputDir)
    	// set target schema
    	.forTable(SCHEMA)
    	// set CQL statement to put data
    	.using(INSERT_STMT)
    	// set partitioner if needed
    	// default is Murmur3Partitioner so set if you use different one.
    	.withPartitioner(new Murmur3Partitioner());
    	CQLSSTableWriter writer = builder.build();

    	for (String ticker : args){
    		lineCounter = 0;
    		try{
    			reader = new BufferedReader(new FileReader(ticker));
				csvReader = new CsvListReader(reader, new CsvPreference.Builder('"', '|', "\r\n").build());
				// Write to SSTable while reading data
    			while ((line = csvReader.read()) != null){
    				// We use Java types here based on
    				// http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
    				if(((++lineCounter)%10000)==0){
    					System.out.println(ticker+" -> "+lineCounter+" line(s) read.");
    				}
    				try {
    					writer.addRow(
    							line.get(0),// visitor_id varchar,
    							line.get(1),//visit_id varchar,
    							line.get(3),//product_id varchar,
    							(line.get(2)==null ?  null : Integer.parseInt(line.get(2))),//is_visit_converted int,
    							line.get(4),//ts varchar,
    							(line.get(5)==null ?  null : Integer.parseInt(line.get(5))),//view_quantity int,
    							(line.get(6)==null ?  null : Integer.parseInt(line.get(6))),//basket_quantity int,
    							(line.get(7)==null ?  null : Integer.parseInt(line.get(7))),//order_quantity int,
    							(line.get(8)==null ?  null : Integer.parseInt(line.get(8))),//stock int,
    							(line.get(9)==null ?  null : Double.parseDouble(line.get(9))),//unit_price Double,
    							(line.get(10)==null ?  null : Double.parseDouble(line.get(10))),//unit_sale_price Double,
    							(line.get(11)==null ?  null : Double.parseDouble(line.get(11))),//unit_sale_percent Double,
    							(line.get(12)==null ?  null : Double.parseDouble(line.get(12))),//unit_shipping_price Double,
    							line.get(13));//shipping_time string,
    					totalRecord++;
    				} catch (InvalidRequestException e) {
    					e.printStackTrace();
    				} catch (NumberFormatException e) {
    					e.printStackTrace();
    				}
    			}
				System.out.println(ticker+" -> "+lineCounter+" line(s) read.");
    		}catch (IOException e){
    			e.printStackTrace();
    		}
    	}
    	System.out.println(">>> Total Records = "+totalRecord);
    	try{
    		writer.close();
    	}catch (IOException ignore) {}
    }
    
//    visitor_id,			0
//    visit_id,				1
//    is_visit_converted,	2
//    product_id,			3
//    ts,					4
//    view_quantity,		5
//    basket_quantity,		6
//    order_quantity,		7
//    stock int,			8
//    unit_price Double,	9
//    unit_sale_price Double,	10
//    unit_sale_percent Double,	11
//    unit_shipping_price Double,	12


// cd C:\Users\cbremard\Documents\workspaceGithub\cbulkload
// mvn clean compile assembly:single
// pscp -i C:\Users\cbremard\Downloads\ensai.ppk .\target\cbulkload-jar-with-dependencies.jar ubuntu@54.217.148.232:/home/ubuntu/corentin/cbulkload_PFEensai2015.jar
// java -jar cbulkload_PFEensai2015.jar ref_product netaffadvaloabbmfd8z2.csv
// java -jar cbulkload_PFEensai2015.jar visitor_product part-r-00000
// sstableloader -d 127.0.0.1 data/abb/ref_product
// sstableloader -d 127.0.0.1 data/abb/visitor_product

 // scp -i ensai.pem part-* ubuntu@54.74.21.141:/home/ubuntu/rawdata/






//    public static void generateSSTABLE2(File outputDir, String[] args){
//    	int counter=0;
//    	SSTableSimpleUnsortedWriter eventWriter = 
//    			new SSTableSimpleUnsortedWriter(outputDir, new Murmur3Partitioner(), KEYSPACE, TABLE, AsciiType.instance,null, 64);
//
//    	for (String ticker : args){
//    		try (BufferedReader reader = new BufferedReader(new FileReader(ticker));
//    			CsvListReader csvReader = new CsvListReader(reader, new CsvPreference.Builder('"', '|', "\r\n").build()))
//    			{
//	    			// Write to SSTable while reading data
//	    			List<String> line;
//	    			while ((line = csvReader.read()) != null){
//	    	    		if(++counter>100){
//	    	    			break;
//	    	    		}
//	    				System.out.println(ticker+" : "+line.get(0)+" - "+line.get(1)+" - "+line.get(2));
//	    				// We use Java types here based on
//	    				// http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
//	    				try {
//	    					long timestamp = System.currentTimeMillis();
//	    					eventWriter.newRow(ByteBufferUtil.bytes(line.get(0)));
//	    					eventWriter.addColumn(ByteBufferUtil.bytes("visit_id"),ByteBufferUtil.bytes(line.get(1)), timestamp);
//	    					eventWriter.addColumn(ByteBufferUtil.bytes("product_id"),ByteBufferUtil.bytes(line.get(2)), timestamp);
//	    					if(line.get(3) != null && line.get(3).length()>0){
//	    						int value = Integer.parseInt(line.get(3));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("is_visit_converted"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(4) != null && line.get(4).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("ts"),ByteBufferUtil.bytes(line.get(4)), timestamp);
//	    					}
//	    					if(line.get(5) != null && line.get(5).length()>0){
//	    						int value = Integer.parseInt(line.get(5));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("view_quantity"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(6) != null && line.get(6).length()>0){
//	    						int value = Integer.parseInt(line.get(6));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("basket_quantity"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(7) != null && line.get(7).length()>0){
//	    						int value = Integer.parseInt(line.get(7));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("order_quantity"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(8) != null && line.get(8).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("sku"),ByteBufferUtil.bytes(line.get(8)), timestamp);
//	    					}
//	    					if(line.get(9) != null && line.get(9).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("name"),ByteBufferUtil.bytes(line.get(9)), timestamp);
//	    					}
//	    					if(line.get(10) != null && line.get(10).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("description"),ByteBufferUtil.bytes(line.get(10)), timestamp);
//	    					}
//	    					if(line.get(11) != null && line.get(11).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_main"),ByteBufferUtil.bytes(line.get(11)), timestamp);
//	    					}
//	    					if(line.get(12) != null && line.get(12).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_sub_1"),ByteBufferUtil.bytes(line.get(12)), timestamp);
//	    					}
//	    					if(line.get(13) != null && line.get(13).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_sub_2"),ByteBufferUtil.bytes(line.get(13)), timestamp);
//	    					}
//	    					if(line.get(14) != null && line.get(14).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_sub_3"),ByteBufferUtil.bytes(line.get(14)), timestamp);
//	    					}
//	    					if(line.get(15) != null && line.get(15).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_sub_4"),ByteBufferUtil.bytes(line.get(15)), timestamp);
//	    					}
//	    					if(line.get(16) != null && line.get(16).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("categ_sub_5"),ByteBufferUtil.bytes(line.get(16)), timestamp);
//	    					}
//	    					if(line.get(17) != null && line.get(17).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("manufacturer"),ByteBufferUtil.bytes(line.get(17)), timestamp);
//	    					}
//	    					if(line.get(18) != null && line.get(18).length()>0){
//	    						int value = Integer.parseInt(line.get(18));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("stock"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(19) != null && line.get(19).length()>0){
//	    						double value = Double.parseDouble(line.get(19));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("unit_price"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(20) != null && line.get(20).length()>0){
//	    						double value = Double.parseDouble(line.get(20));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("unit_sale_price"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(21) != null && line.get(21).length()>0){
//	    						double value = Double.parseDouble(line.get(21));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("unit_sale_percent"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(22) != null && line.get(22).length()>0){
//	    						double value = Double.parseDouble(line.get(22));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("unit_shipping_price"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(23) != null && line.get(23).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("shipping_time"),ByteBufferUtil.bytes(line.get(23)), timestamp);
//	    					}
//	    					if(line.get(24) != null && line.get(24).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("url"),ByteBufferUtil.bytes(line.get(24)), timestamp);
//	    					}
//	    					if(line.get(25) != null && line.get(25).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("url_img_low"),ByteBufferUtil.bytes(line.get(25)), timestamp);
//	    					}
//	    					if(line.get(26) != null && line.get(26).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("url_img_med"),ByteBufferUtil.bytes(line.get(26)), timestamp);
//	    					}
//	    					if(line.get(27) != null && line.get(27).length()>0){
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("url_img_high"),ByteBufferUtil.bytes(line.get(27)), timestamp);
//	    					}
//	    					if(line.get(28) != null && line.get(28).length()>0){
//	    						int value = Integer.parseInt(line.get(28));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("is_active"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    					if(line.get(29) != null && line.get(29).length()>0){
//	    						int value = Integer.parseInt(line.get(29));
//	    						eventWriter.addColumn(ByteBufferUtil.bytes("is_excluded"),ByteBufferUtil.bytes(value), timestamp);
//	    					}
//	    				} catch (NumberFormatException e) {
//	    					e.printStackTrace();
//	    				}
//	    			}
//				}catch (IOException e){
//					e.printStackTrace();
//				}
//    	}
//    	try {
//			eventWriter.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//    }
}
