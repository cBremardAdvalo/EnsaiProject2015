package scraping;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	
    public static void main( String[] args )
    {
    	LOGGER.debug("START");
    	String page = "http://www.ubaldi.com/puericulture/sommeil/decoration-chambre/babycalin/fauteuil-enfant-babycalin--fauteuil-club-mango--660330.php";
        try {
			Document doc = Jsoup.connect(page).get();
			String data = "For product page \""+page+"\", we have meta data :";
			for(Element meta : doc.select("div#dv_ean")) {
				if(meta.hasAttr("itemprop")){
					data += "\r\n\t\""+meta.attr("id")+"\" = "+meta.attr("title");
				}
//				if(meta.hasAttr("itemprop")){
//					data += "\r\n\t\""+meta.attr("itemprop")+"\" = "+meta.attr("content");
//				}
			}
			LOGGER.info(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	LOGGER.debug("END");
    }
}
