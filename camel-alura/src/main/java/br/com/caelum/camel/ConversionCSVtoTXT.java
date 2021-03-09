package br.com.caelum.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.CsvDataFormat;

public class ConversionCSVtoTXT {

	public static void main(String[] args) throws Exception {

			CamelContext context = new DefaultCamelContext();
			
			context.addRoutes(new RouteBuilder() { //cuidado, não é RoutesBuilder
	
			    @Override
			    public void configure() {
		            try {
		                CsvDataFormat csvDataFormat = new CsvDataFormat();
		                csvDataFormat.setHeaderDisabled(true);
		                csvDataFormat.setDelimiter("|");


		                from("file:pastaComCSV").
		                        unmarshal().
		                        csv().
		                        marshal(csvDataFormat).
		                        setHeader("CamelFileName", simple("${file:name.noext}.txt")).
		                        to("file:pastaSaidaCSVvirouTXT")
		                        .end();
		            } catch (Exception e) {
		                e.printStackTrace();
		            }
		        }
	
		});	
			
			context.start();
			Thread.sleep(20000);
			context.stop();
}
}
