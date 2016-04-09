package au.com.anz.iib.commons.compute;
import au.com.anz.utils.TerminalAssemblyPair;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;


public abstract class CommonMbJavaComputeNode extends MbJavaComputeNode {
	
	public abstract TerminalAssemblyPair execute(MbMessageAssembly inAssembly) throws Exception;

	public MbOutputTerminal getOutTerminal() {
		return getOutputTerminal("out");
	}
	
	public MbOutputTerminal getAltTerminal() {
		return getOutputTerminal("alt");
	}

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {

		try {			
			TerminalAssemblyPair pair = execute(inAssembly);
			pair.getKey().propagate(pair.getValue());

		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
	}

}
