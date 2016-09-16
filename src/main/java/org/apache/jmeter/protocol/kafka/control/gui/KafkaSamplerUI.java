package org.apache.jmeter.protocol.kafka.control.gui;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.jmeter.gui.util.JSyntaxTextArea;
import org.apache.jmeter.gui.util.JTextScrollPane;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.protocol.kafka.sampler.KafkaSampler;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextField;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

@SuppressWarnings("deprecation")
public class KafkaSamplerUI extends AbstractSamplerGui {
	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggingManager.getLoggerForClass();
//	private final JLabeledTextField brokersField = new JLabeledTextField(JMeterUtils.getResString("kafka.brokers", "Brokers"));
//	private final JLabeledTextField topicField = new JLabeledTextField(JMeterUtils.getResString("kafka.topic", "Topic"));
//	private final JLabeledTextField keyField = new JLabeledTextField(JMeterUtils.getResString("kafka.key", "Key"));
//	private final JLabeledTextField messageSerializerField = new JLabeledTextField(JMeterUtils.getResString("kafka.message.serializer", "Message Serializer"));
//	private final JLabeledTextField keySerializerField = new JLabeledTextField(JMeterUtils.getResString("kafka.key.serializer", "Key Serializer"));

	private final JLabeledTextField brokersField = new JLabeledTextField("Brokers");
	private final JLabeledTextField topicField = new JLabeledTextField("Topic");
	//private final JLabeledTextField keyField = new JLabeledTextField("Key");
	private final JLabeledTextField messageSerializerField = new JLabeledTextField("Message Serializer");
	private final JLabeledTextField keySerializerField = new JLabeledTextField("Key Serializer");
	
	private final JSyntaxTextArea textMessage = new JSyntaxTextArea(10, 50);
//	private final JLabel textArea = new JLabel(JMeterUtils.getResString("kafka.message", "Message"));
	private final JLabel textArea = new JLabel("Message");
	private final JTextScrollPane textPanel = new JTextScrollPane(textMessage);

	public KafkaSamplerUI() {
		super();
		this.init();
	}

	private void init() {
		log.info("Initializing the UI.");
		setLayout(new BorderLayout());
		setBorder(makeBorder());

		add(makeTitlePanel(), BorderLayout.NORTH);
		JPanel mainPanel = new VerticalPanel();
		add(mainPanel, BorderLayout.CENTER);

		JPanel DPanel = new JPanel();
		DPanel.setLayout(new GridLayout(3, 2));
		DPanel.add(brokersField);
		DPanel.add(topicField);
		//DPanel.add(keyField);
		DPanel.add(messageSerializerField);
		DPanel.add(keySerializerField);

		JPanel ControlPanel = new VerticalPanel();
		ControlPanel.add(DPanel);
		ControlPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.gray), "Parameters"));
		mainPanel.add(ControlPanel);

		JPanel ContentPanel = new VerticalPanel();
		JPanel messageContentPanel = new JPanel(new BorderLayout());
		messageContentPanel.add(this.textArea, BorderLayout.NORTH);
		messageContentPanel.add(this.textPanel, BorderLayout.CENTER);
		ContentPanel.add(messageContentPanel);
		ContentPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.gray), "Content"));
		mainPanel.add(ContentPanel);
	}

	@Override
	public TestElement createTestElement() {
		KafkaSampler sampler = new KafkaSampler();
		this.setupSamplerProperties(sampler);
		return sampler;
	}
	
	@Override
	public void clearGui() {
		super.clearGui();
		this.brokersField.setText("kafka_server:9092");
		this.topicField.setText("jmeterTest");
		//this.keyField.setText("");
		this.messageSerializerField.setText("kafka.serializer.StringEncoder");
		this.keySerializerField.setText("kafka.serializer.StringEncoder");
		this.textMessage.setText("");
	}

	@Override
	public void configure(TestElement element) {
		super.configure(element);
		KafkaSampler sampler = (KafkaSampler)element;
		this.brokersField.setText(sampler.getBrokers());
		this.topicField.setText(sampler.getTopic());
		//this.keyField.setText(sampler.getKey());
		this.messageSerializerField.setText(sampler.getMessageSerializer());
		this.keySerializerField.setText(sampler.getKeySerializer());
		this.textMessage.setText(sampler.getMessage());
	}

	private void setupSamplerProperties(KafkaSampler sampler) {
		this.configureTestElement(sampler);
		sampler.setBrokers(this.brokersField.getText());
		sampler.setTopic(this.topicField.getText());
		//sampler.setKey(this.keyField.getText());
		sampler.setMessage(this.textMessage.getText());
		sampler.setMessageSerializer(this.messageSerializerField.getText());
		sampler.setKeySerializer(this.keySerializerField.getText());
	}
	
	@Override
	public String getStaticLabel() {
		return "Kafka Sampler";
	}

	@Override
	public String getLabelResource() {
		throw new IllegalStateException("This shouldn't be called"); 
	}

	@Override
	public void modifyTestElement(TestElement testElement) {
		KafkaSampler sampler = (KafkaSampler) testElement;
		this.setupSamplerProperties(sampler);
	}
}
