package scraping;


public class Main {

	public static void main(String[] args) throws Exception {
		if(args!=null && args.length>1){
			String[] newArgs = new String[args.length-1];
			for (int i = 1; i < args.length; i++) {
				newArgs[i-1] = args[i];
			}
			if(args[0].toLowerCase().equals("ref_product")){
				(new REF_PRODUCT()).run(newArgs);
			}else if(args[0].toLowerCase().equals("visitor_product")){
				(new VISITOR_PRODUCT()).run(newArgs);
			}
		}else{
			throw new Exception("Unexpected input");
		}

	}

}
