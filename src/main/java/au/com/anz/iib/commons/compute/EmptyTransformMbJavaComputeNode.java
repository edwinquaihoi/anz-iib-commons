package au.com.anz.iib.commons.compute;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import au.com.anz.utils.TerminalAssemblyPair;

import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;

public class EmptyTransformMbJavaComputeNode extends CommonMbJavaComputeNode {

	private static Logger logger = LogManager.getLogger();

	@Override
	public TerminalAssemblyPair execute(MbMessageAssembly inAssembly) throws Exception {
		
		TerminalAssemblyPair terminalAssemblyPair = new TerminalAssemblyPair();
		
		// create new message as a copy of the input
		MbMessage outMessage = new MbMessage(inAssembly.getMessage());
		MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);
		
		
		terminalAssemblyPair.setMessageAssembly(outAssembly);
		terminalAssemblyPair.setOutputTerminal(getOutputTerminal("out"));

		return terminalAssemblyPair;
	}

}
