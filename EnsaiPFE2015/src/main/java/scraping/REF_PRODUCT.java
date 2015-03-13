package scraping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

public class REF_PRODUCT {

    public static final String CSV_URL = "http://real-chart.finance.yahoo.com/table.csv?s=%s";

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /** Keyspace name */
    public static final String KEYSPACE = "abb";
    /** Table name */
    public static final String TABLE = "ref_product";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */    
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s ("
    		+ "product_id text,"
    		+ "name varchar,"
      		+ "description text,"
      		+ "manufacturer varchar,"
      		+ "category varchar,"
      		+ "category_id varchar,"
      		+ "currency varchar,"
      		+ "unit_sale_price double,"
      		+ "unit_price double,"
      		+ "unit_shipping_price double,"
      		+ "ean varchar,"
      		+ "stock int,"
      		+ "shipping_time varchar,"
      		+ "url varchar,"
      		+ "url_img_low varchar,"
      		+ "url_img_med varchar,"
      		+ "url_img_high varchar,"
    		  + "PRIMARY KEY ((product_id))"
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
    public static final String INSERT_STMT = String.format("INSERT INTO  %s.%s "
    		+ "(product_id,name,description,manufacturer,category,category_id,currency,unit_sale_price,unit_price,unit_shipping_price,ean,stock,shipping_time,url,url_img_low,url_img_med,url_img_high) "
    		+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", KEYSPACE, TABLE);

    public void run(String[] args){
    	
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
        
    }
    
    

    private void generateSSTABLE1(File outputDir, String[] args){
		CsvListReader csvReader;
    	BufferedReader reader;
    	CQLSSTableWriter.Builder builder;
    	int lineCounter, totalRecord = 0;
    	List<String> line;
    	
    	Map<String,Integer> position = new HashMap<String,Integer>();
 		position.put("name",				0);//"nom";					0
		position.put("product_id",			1);//"numero";				1
		position.put("manufacturer",			2);//"marque";				2
		position.put("reference",		3);//"reference";			3
		position.put("unit_price",			4);//"prix";				4
		position.put("unit_shipping_price",		5);//"frais de port";		5
		position.put("currency",			6);//"monnaie";				6
		position.put("category",		7);//"categorie";			7
		position.put("description",		8);//"descriptif";			8
		position.put("url",				9);//"url";					9
		position.put("stock",			10);//"stock";				10
		position.put("url_img_low",		11);//"image100";			11
		position.put("url_img_med",		12);//"image150";			12
		position.put("url_img_high",		13);//"image600";			13
		position.put("ean",				14);//"ean";				14
		position.put("shipping_time",15);//"d�lai de livraison";	15
		position.put("unit_sale_price",		16);//"prix barr�";			16
		position.put("category_id",	17);//"id_categorie"		17
		
    	
    	builder = CQLSSTableWriter.builder();
    	builder.inDirectory(outputDir).forTable(SCHEMA).using(INSERT_STMT).withPartitioner(new Murmur3Partitioner());
    	CQLSSTableWriter writer = builder.build();

    	for (String ticker : args){
    		lineCounter = 0;
    		try{
    			reader = new BufferedReader(new FileReader(ticker));
				csvReader = new CsvListReader(reader, new CsvPreference.Builder('"', ';', "\n").build());
				csvReader.read();
				while ((line = csvReader.read()) != null){
    				if(((++lineCounter)%10000)==0){
    					System.out.println(ticker+" -> "+lineCounter+" line(s) read.");
    				}else{
    					
    				}
    				try {
    					writer.addRow(
    							line.get(position.get("product_id")).toLowerCase(),
    							line.get(position.get("name")),
    							line.get(position.get("description")),
    							line.get(position.get("manufacturer")),
    							line.get(position.get("category")),
    							line.get(position.get("category_id")),
    							line.get(position.get("currency")),
    							(line.get(position.get("unit_sale_price"))==null ?  null : Double.parseDouble(line.get(position.get("unit_sale_price")))),
    							(line.get(position.get("unit_price"))==null ?  null : Double.parseDouble(line.get(position.get("unit_price")))),
    							(line.get(position.get("unit_shipping_price"))==null ?  null : Double.parseDouble(line.get(position.get("unit_shipping_price")))),
    							line.get(position.get("ean")),
    							(line.get(position.get("stock"))==null ?  null : Integer.parseInt(line.get(position.get("stock")))),
    							line.get(position.get("shipping_time")),
    							line.get(position.get("url")),
    							line.get(position.get("url_img_low")),
    							line.get(position.get("url_img_med")),
    							line.get(position.get("url_img_high")));
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
}
