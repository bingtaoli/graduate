package test;

import org.rosuda.JRI.Rengine;

import utils.MP;

public class RTest {
	
	public void testEMD(Rengine re){
		//test emd
		re.eval("ndata <- 3000");
		re.eval("tt22 <- seq(0, 9, length=ndata)");
		MP.logln("ndata is: " +  re.eval("ndata"));
		re.eval("xt22 <- sin(pi * tt22) + sin(2* pi * tt22) + sin(6 * pi * tt22) ");
		re.eval("try22 <- emd(xt22, tt22, boundary=\"none\")");
		MP.logln("try22 is: " +  re.eval("try22"));
		MP.logln("try22 imfs length is: " +  re.eval("try22$nimf"));
		return;
	}
}
