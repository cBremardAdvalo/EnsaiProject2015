package scraping;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnection {
	
	private Session session;
	private Cluster cluster;
	public static void main(String[] args) {
		String serverIp = "54.217.148.232";
	    String keyspace = "abb";
	    CassandraConnection connection = new CassandraConnection(serverIp, keyspace);
	    try {
			connection.loadRefProduct();
			connection.loadVisitorProduct();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connection.close();
		}

	}
	
	
	private void close() {
		session.close();
		cluster.close();
	}


	public CassandraConnection(String serverIp, String keyspace) {
		super();
	    cluster = Cluster.builder()
	            .addContactPoints(serverIp)
	            .build();
	    session = cluster.connect(keyspace);
	}


	private void loadRefProduct() throws IOException {
		int counter = 0;
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\cbremard\\Downloads\\ABB_ReferentielProduit_20150209.csv"));
		@SuppressWarnings("resource")
		CsvListReader csvReader = new CsvListReader(reader, new CsvPreference.Builder('"', ';', "\n").build());
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
		
		line = csvReader.read();
		
		while ((line = csvReader.read()) != null){
			counter++;
			if((counter%100)==1){
				System.out.println("\nref_product : "+counter+" -> "+StringUtils.join(line.iterator(), "|"));
			}else{
				System.out.print(".");
			}
			session.execute("INSERT INTO  abb.ref_product "
					+ "(product_id,name,description,manufacturer,category,category_id,"
					+ "currency,unit_sale_price,unit_price,unit_shipping_price,"
					+ "ean,stock,shipping_time,url,url_img_low,url_img_med,url_img_high) VALUES "
					+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", 
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
		}

		System.out.println("\n"+counter+" lines exported.");
	}
	

	private void loadVisitorProduct() throws IOException {
		int counter = 0;
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\cbremard\\Downloads\\part-r-00000"));
		@SuppressWarnings("resource")
		CsvListReader csvReader = new CsvListReader(reader, new CsvPreference.Builder('"', ';', "\n").build());
		List<String> line;
		
		line = csvReader.read();
		
		while ((line = csvReader.read()) != null){
			counter++;
			if((counter%100)==1){
				System.out.println("\nvisitor_product : "+counter+" -> "+StringUtils.join(line.iterator(), "|"));
			}else{
				System.out.print(".");
			}
			session.execute("INSERT INTO  abb.visitor_product "
    		+ "(visitor_id,visit_id,product_id,is_visit_converted,ts,view_quantity,basket_quantity,order_quantity,stock,unit_price,unit_sale_price,unit_sale_percent,unit_shipping_price,shipping_time) "
    		+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);", 
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
					line.get(13));
		}
		System.out.println("\n"+counter+" lines exported.");
	}

}
