package br.com.caelum.camel;

import java.text.SimpleDateFormat;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;

import com.thoughtworks.xstream.XStream;
public class RotaNegociacoesTimer {

	public static void main(String[] args) throws Exception {

		SimpleRegistry registro = new SimpleRegistry();
		registro.put("mysql", criaDataSource());
		CamelContext context = new DefaultCamelContext(registro);//construtor recebe registro
		
		context.addRoutes(new RouteBuilder() { 

		    @Override
		    public void configure() throws Exception {
		    	
		    	final XStream xstream = new XStream();
		    	//criando representacao do XML
		    	xstream.alias("negociacao", Negociacao.class);
		    	
		    	//definindo componente timer com parametros de periodo
		    	from("timer://negociacoes?fixedRate=true&delay=1s&period=10s").
		        
		    	//definindo a rota http que sera obtido os dados
		    	to("http4://argentumws-spring.herokuapp.com/negociacoes").
		    	
	            //Convertendo retorno para a representação do objeto
	            unmarshal(new XStreamDataFormat(xstream)).
		            
	            split(body()). //cada negociação torna uma mensagem
		            
		            process(new Processor() {
		                @Override
		                public void process(Exchange exchange) throws Exception {
		                    Negociacao negociacao = exchange.getIn().getBody(Negociacao.class);
		                    exchange.setProperty("preco", negociacao.getPreco());
		                    exchange.setProperty("quantidade", negociacao.getQuantidade());
		                    String data = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(negociacao.getData().getTime());
		                    exchange.setProperty("data", data);
		                }
		              }).
		            setBody(simple("insert into negociacao(preco, quantidade, data) values (${property.preco}, ${property.quantidade}, '${property.data}')")).
		            log("${body}"). //logando o comando sql
		            delay(1000). //esperando 1s para deixar a execução mais fácil de entender
		        to("jdbc:mysql"); //usando o componente jdbc que envia o SQL para mysql
			    	
			    }	
		});	
			
			context.start();
			Thread.sleep(200000);
			context.stop();
}
	
	private static MysqlConnectionPoolDataSource criaDataSource() throws Exception {
	    MysqlConnectionPoolDataSource mysqlDs = new MysqlConnectionPoolDataSource();
	    mysqlDs.setDatabaseName("camel");
	    mysqlDs.setServerName("localhost");
	    mysqlDs.setPort(3306);
	    mysqlDs.setUser("root");
	    mysqlDs.setPassword("!QAZ2wsx");
	    mysqlDs.setServerTimezone("UTC");
	    
	    return mysqlDs;
	}
	
	
}
