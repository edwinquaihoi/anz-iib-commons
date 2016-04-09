package au.com.anz.iib.commons.compute;

import au.com.anz.utils.TerminalAssemblyPair;

import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;

public class BlobTransformMbJavaComputeNode extends CommonMbJavaComputeNode {


	@Override
	public TerminalAssemblyPair execute(MbMessageAssembly inAssembly) throws Exception {
		
		TerminalAssemblyPair terminalAssemblyPair = new TerminalAssemblyPair();
		
		// create new message as a copy of the input
		MbMessage outMessage = new MbMessage(inAssembly.getMessage());
		MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);
		
		
		// get json blob from message
		MbElement jsonBlob = inAssembly.getMessage().getRootElement ().getFirstElementByPath("/BLOB/BLOB");
		byte[] jsonBlobBytes = new byte[]{};
		
		if(jsonBlob != null) {
			jsonBlobBytes = (byte[]) jsonBlob.getValue();
		}
		
		// use the name of the node to identify the transform class		
		IBlobTransformer blobTransformer = (IBlobTransformer)Class.forName(getName()).newInstance();
		byte[] transformedJsonBlobBytes = blobTransformer.execute(jsonBlobBytes);
		
		// update the outMessage json blob
		// FIXME what happens when no BLOB in message?
		if(jsonBlob != null) {
			outMessage.getRootElement().getFirstElementByPath("/BLOB/BLOB").setValue(transformedJsonBlobBytes);			
		} else {
			// add a blob message
			MbElement blobParent = outMessage.getRootElement().createElementAsLastChild("BLOB");
			MbElement blobChild = blobParent.createElementAsLastChild("BLOB");
			blobChild.setValue(transformedJsonBlobBytes);			
		}
		
		terminalAssemblyPair.setMessageAssembly(outAssembly);
		terminalAssemblyPair.setOutputTerminal(getOutputTerminal("out"));

		return terminalAssemblyPair;
	}

}
