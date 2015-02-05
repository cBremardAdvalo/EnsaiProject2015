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
        try {
			Document doc = Jsoup.connect("http://www.aubert.com/Lits-60-x-120-cm-transformables-Lit-Sleepi-120-cm-Noyer-Stokke.html").get();
			String data = "For product page \"/www.aubert.com/Lits-60-x-120-cm-transformables-Lit-Sleepi-120-cm-Noyer-Stokke.html\", we have meta data :";
			for(Element meta : doc.select("meta")) {
				if(meta.hasAttr("itemprop")){
					data += "\r\n\t\""+meta.attr("itemprop")+"\" = "+meta.attr("content");
				}
			}
			LOGGER.info(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	LOGGER.debug("END");
    }
}
