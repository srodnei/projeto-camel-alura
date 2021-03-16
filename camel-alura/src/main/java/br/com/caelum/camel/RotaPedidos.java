package br.com.caelum.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http4.HttpMethods;
import org.apache.camel.impl.DefaultCamelContext;

public class RotaPedidos {

	public static void main(String[] args) throws Exception {

			CamelContext context = new DefaultCamelContext();
			
			context.addRoutes(new RouteBuilder() { 
	
			    @Override
			    public void configure() throws Exception {
			    	from("file:pedidos").
			    	setProperty("pedidoId", xpath("/pedido/id/text()")).
			    	setProperty("clienteId", xpath("/pedido/pagamento/email-titular/text()")).
			    	log("${id}").
			    	split().xpath("/pedido/itens/item").
			    	
			    	setProperty("ebookId", xpath("/item/livro/codigo/text()")).
			    	
			    	filter().xpath("/item/formato[text()='EBOOK']").
			    		log("${id}").
			    		marshal().xmljson().
			    		log("${id} - ${body}").			    	  
			    		
			    		//setHeader("CamelFileName", simple("${file:name.noext}.json")).
			    		//setHeader(Exchange.FILE_NAME, simple("${file:name.noext}-${header.CamelSplitIndex}.json")).
			    		setHeader(Exchange.HTTP_METHOD, HttpMethods.GET).
			    		setHeader(Exchange.HTTP_QUERY, simple("ebookId=${property.ebookId}&pedidoId=${property.pedidoId}&clienteId=${property.clienteId}")).
			    		
			    	//to("file:saida");
			    	to("http4://localhost:8080/webservices/ebook/item");
			    }
	
		});	
			
			context.start();
			Thread.sleep(20000);
			context.stop();
}
}
