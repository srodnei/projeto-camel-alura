package br.com.caelum.camel;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;

public class RotaPedidos {

	public static void main(String[] args) throws Exception {

			CamelContext context = new DefaultCamelContext();
			//Instanciando a fila o servidor MQ que será consumido
			context.addComponent("activemq", ActiveMQComponent.activeMQComponent("tcp://localhost:61616/"));
			context.addRoutes(new RouteBuilder() { 
	
			    @Override
			    public void configure() throws Exception {
			    	
			    	//Captura o erro, cria pasta e joga arquivo nao processado
			    	//errorHandler(deadLetterChannel("file:erro").
			    	
			    	//Captura o erro, joga arquivo nao processado na fila pedidos.DLQ (criada pela interface http://localhost:8161/admin/queues.jsp)
			    	errorHandler(deadLetterChannel("activemq:queue:pedidos.DLQ").
			    			
			    			
			    			//exibe no log arquivo retirado com erro e retirado da fila
			    			logExhaustedMessageHistory(true).
			    			//tenta por 3 vezes reprocessar o arquivo.. - usar em casos que uma nova tentativa possa surtir efeito (rede, dns..)
			    			maximumRedeliveries(3).
			    			//entre uma tentativa e outra aguarda 2 segundos
			    			redeliveryDelay(2000).
			    			onRedelivery(new Processor() {
								//Imprimi no log a cada tentativa realizada
								@Override
								public void process(Exchange exchange) throws Exception {
									
									int counter = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER);
									int max = (int) exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER);
									System.out.println("Redelivery " + counter + "/" + max);
									
								}
							}));
			    	
			    	//Consome arquivo XML da pasta pedidos a cada 5 segundos sem exclusao
			    	//from("file:pedidos?delay=5s&noop=true").
			    	from("activemq:queue:pedidos").
			    	routeId("rota-pedidos").
			        to("validator:pedido.xsd").
			        //multicast().
			            //to("direct:soap").
			            //to("direct:http");

		    	from("direct:soap").
			        routeId("rota-soap").
			    to("xslt:pedido-para-soap.xslt"). 
			        log("Resultado do Template: ${body}").
			        setHeader(Exchange.CONTENT_TYPE, constant("text/xml")).
		        to("http4://localhost:8080/webservices/financeiro");

			    from("direct:http").
			        routeId("rota-http").
			        setProperty("pedidoId", xpath("/pedido/id/text()")).
			        setProperty("email", xpath("/pedido/pagamento/email-titular/text()")).
			        split().
			            xpath("/pedido/itens/item").
			        filter().
			            xpath("/item/formato[text()='EBOOK']").
			        setProperty("ebookId", xpath("/item/livro/codigo/text()")).
			        setHeader(Exchange.HTTP_QUERY,
			                simple("clienteId=${property.email}&pedidoId=${property.pedidoId}&ebookId=${property.ebookId}")).
			    to("http4://localhost:8080/webservices/ebook/item");
			    				    				    
			    }
	
		});	
			
			context.start();
			Thread.sleep(20000);
			context.stop();
}
}
