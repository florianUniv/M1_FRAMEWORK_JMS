package fr.pantheonsorbonne.ufr27.miage.jms;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import fr.pantheonsorbonne.ufr27.miage.DiplomaInfo;
import fr.pantheonsorbonne.ufr27.miage.dto.BinaryDiplomaDTO;

@ApplicationScoped
public class BinaryDiplomaManager implements Closeable {

	@Inject
	@Named("diplomaRequests")
	private Queue requestsQueue;

	@Inject
	@Named("diplomaFiles")
	private Queue filesQueue;

	@Inject
	private ConnectionFactory connectionFactory;

	private Connection connection;
	private MessageConsumer binDiplomaConsumer;
	private MessageProducer diplomaRequestProducer;

	private Session session;

	@PostConstruct
	void init() {
		try {
			connection = connectionFactory.createConnection("nicolas", "nicolas");
			connection.start();
			session = connection.createSession();
			binDiplomaConsumer = session.createConsumer(filesQueue);
			diplomaRequestProducer = session.createProducer(requestsQueue);
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}

	}

	public BinaryDiplomaDTO consume() {

		try {
			
			// receive a Byte Message from the consumer
			BytesMessage message = (BytesMessage) binDiplomaConsumer.receive();
			
			// create a byte array sized after the message's payload body length
			byte[] payload = new byte[(int) message.getBodyLength()];
			
			// read the message on the byte array
			message.readBytes(payload);
			
			// create a BinaryDiplomaDTO containing the id of the diploma and the data
			BinaryDiplomaDTO dto = new BinaryDiplomaDTO();
			dto.setId(message.getIntProperty("id"));
			dto.setData(payload);
			
			// return the DTO
			return dto;
			
		} catch (JMSException e) {
			System.out.println("failed to consume message ");
			return null;
		}

		//return null;
	}

	public void requestBinDiploma(DiplomaInfo info) {

		try {
			// create a String writer
			StringWriter writer = new StringWriter();
			
			// create a JaxBContext, and bount DiplomaInfo.class
			JAXBContext jaxbContext = JAXBContext.newInstance(DiplomaInfo.class);
			
			// create a Marshaller and marshall the class in the writer
			jaxbContext.createMarshaller().marshal(info, writer);
			
			// send a text Message containing the JAXB-marshalled object through the wire
			this.diplomaRequestProducer.send(this.session.createTextMessage(writer.toString()));
			
		} catch (JAXBException e) {
			System.err.println("failed to marshall diploma info : " + info.toString());
		} catch (JMSException e) {
			System.err.println("failed to send diploma Request");
		}
	}

	@Override
	public void close() throws IOException {
		try {
			binDiplomaConsumer.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			System.out.println("Failed to close JMS resources");
		}

	}

}
