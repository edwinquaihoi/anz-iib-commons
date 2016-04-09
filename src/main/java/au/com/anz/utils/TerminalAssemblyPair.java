package au.com.anz.utils;

import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;

public class TerminalAssemblyPair extends Pair<MbOutputTerminal, MbMessageAssembly> {

	public TerminalAssemblyPair() {
		super();
	}
	
	public TerminalAssemblyPair(MbOutputTerminal outputTerminal,MbMessageAssembly messageAssembly) {
		super(outputTerminal, messageAssembly);
	}
	
	public MbOutputTerminal getOutputTerminal() {
		return getKey();		
	}
	
	public void setOutputTerminal(MbOutputTerminal mbOutputTerminal) {
		setKey(mbOutputTerminal);
	}

	MbMessageAssembly getMessageAssembly() {
		return getValue();		
	}
	
	public void setMessageAssembly(MbMessageAssembly mbMessageAssembly) {
		setValue(mbMessageAssembly);
	}
}
