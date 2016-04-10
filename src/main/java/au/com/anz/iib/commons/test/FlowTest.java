package au.com.anz.iib.commons.test;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import com.ibm.broker.config.proxy.AttributeConstants;
import com.ibm.broker.config.proxy.BrokerProxy;
import com.ibm.broker.config.proxy.ConfigManagerProxyLoggedException;
import com.ibm.broker.config.proxy.ConfigManagerProxyPropertyNotInitializedException;
import com.ibm.broker.config.proxy.ExecutionGroupProxy;

public abstract class FlowTest {

	// FIXME make this configurable from system property 
	private static final String BROKER_NODE_NAME = "Node01";
	private static final String INTEGRATION_SERVER_NAME = "svr1";

	private static BrokerProxy brokerNodeProxy;
	private static ExecutionGroupProxy integrationServerProxy;

	@BeforeClass
	public static void initialise() throws ConfigManagerProxyLoggedException, ConfigManagerProxyPropertyNotInitializedException {
		// get broker
		brokerNodeProxy = BrokerProxy.getLocalInstance(BROKER_NODE_NAME);
		
		if(brokerNodeProxy != null) {
			if(!brokerNodeProxy.isRunning()) {
				// stop test execution
				fail("Broker Node " + BROKER_NODE_NAME + " is not running. Please start the Node before running Tests.");
			} else {
				// setup integration server reference				
				integrationServerProxy = brokerNodeProxy.getExecutionGroupByName(INTEGRATION_SERVER_NAME);
				
				if(integrationServerProxy != null) {
					// start integration server
					if(!integrationServerProxy.isRunning()) {
						integrationServerProxy.start();
						
						// enable test injection mode
						integrationServerProxy.setInjectionMode(AttributeConstants.MODE_ENABLED);
						integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_ENABLED);
						
						// TODO find a better way to do event handling of asynchronous calls
						// sleep for a second as calls above are asynchronous
						try { Thread.sleep(1000); } catch (InterruptedException e) { }
					}
				} else {
					fail("Integration Server " + INTEGRATION_SERVER_NAME + " is not configured in Broker Node " + BROKER_NODE_NAME + ". Please configure the Integrat before running Tests.");
				}
			}
		}
	}
	
	@AfterClass
	public static void finalise() throws ConfigManagerProxyPropertyNotInitializedException, ConfigManagerProxyLoggedException {
		integrationServerProxy.clearRecordedTestData();
		integrationServerProxy.setInjectionMode(AttributeConstants.MODE_DISABLED);
		integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_DISABLED);
		
	}
	
	@Before
	public void setup() throws ConfigManagerProxyPropertyNotInitializedException, ConfigManagerProxyLoggedException {
		integrationServerProxy.clearRecordedTestData();

		// enable test injection mode
		integrationServerProxy.setInjectionMode(AttributeConstants.MODE_ENABLED);
		integrationServerProxy.setTestRecordMode(AttributeConstants.MODE_ENABLED);
		
		// TODO find a better way to do event handling of asynchronous calls
		// sleep for a second as calls above are asynchronous
		try { Thread.sleep(1000); } catch (InterruptedException e) { }
	}

	public static BrokerProxy getBrokerNode() {
		return brokerNodeProxy;
	}
	
	public static ExecutionGroupProxy getIntegrationServerProxy() {
		return integrationServerProxy;
	}
}
