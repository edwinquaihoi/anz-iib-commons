package au.com.anz.iib.commons.compute;

import javax.xml.bind.DatatypeConverter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import au.com.anz.utils.TerminalAssemblyPair;

import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;

public class JsonTransformMbJavaComputeNode extends CommonMbJavaComputeNode {

	private static Logger logger = LogManager.getLogger();

	@Override
	public TerminalAssemblyPair execute(MbMessageAssembly inAssembly) throws Exception {
		
		TerminalAssemblyPair terminalAssemblyPair = new TerminalAssemblyPair();
		
		// create new message as a copy of the input
		MbMessage outMessage = new MbMessage(inAssembly.getMessage());
		MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);
		
		// get encoding and ccsid
		/*
		MbElement encoding = inAssembly.getMessage().getRootElement ().getFirstElementByPath("/Properties/Encoding");
		MbElement ccsid = inAssembly.getMessage().getRootElement ().getFirstElementByPath("/Properties/CodedCharSetId");
		
		logger.error(encoding);
		logger.error(ccsid);
		
		logger.error("encoding="+encoding.getValueAsString());
		logger.error("ccsid="+ccsid.getValueAsString());
		*/
		// get json blob from message
		MbElement jsonBlob = inAssembly.getMessage().getRootElement ().getFirstElementByPath("/JSON/Data");
		//logger.error(jsonBlob);
		byte[] jsonBlobBytes = new byte[]{};
		
		if(jsonBlob != null) {
			//jsonBlobBytes = jsonBlob.toBitstream(null, null, null, 546, 1208, 0);
			jsonBlobBytes = DatatypeConverter.parseHexBinary(jsonBlob.getValueAsString());
			
			//logger.error(Arrays.toString(DatatypeConverter.parseHexBinary(jsonBlob.getValueAsString())));
			//logger.error(Arrays.toString(jsonBlobBytes));
			//logger.error(Arrays.toString((byte[])jsonBlob.getValue()));
		}
		
		// use the name of the node to identify the transform class		
		IBlobTransformer blobTransformer = (IBlobTransformer)Class.forName(getName()).newInstance();
		byte[] transformedJsonBlobBytes = blobTransformer.execute(jsonBlobBytes);
		
		// update the outMessage json blob
		// FIXME what happens when no BLOB in message?
		if(jsonBlob != null) {
			outMessage.getRootElement().getFirstElementByPath("/JSON/Data").setValue(transformedJsonBlobBytes);			
		} else {
			// add a blob message
			MbElement blobParent = outMessage.getRootElement().createElementAsLastChild("JSON");
			MbElement blobChild = blobParent.createElementAsLastChild("Data");
			blobChild.setValue(transformedJsonBlobBytes);			
		}
		
		terminalAssemblyPair.setMessageAssembly(outAssembly);
		terminalAssemblyPair.setOutputTerminal(getOutputTerminal("out"));

		return terminalAssemblyPair;
	}
}
