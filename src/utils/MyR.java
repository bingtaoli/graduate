package utils;

import org.rosuda.JRI.Rengine;

public class MyR {
	
	public static Rengine re = null;

	public static Rengine getREngine(){
		if (re != null){
			MP.logln("re is already initialized", 0);
			return re;
		}
		/**
		 * R engine test
		 */
		// just making sure we have the right version of everything
		if (!Rengine.versionCheck()) {
		    System.err.println("** Version mismatch - Java files don't match library version.");
		    System.exit(1);
		}
        System.out.println("Creating Rengine (with arguments)");
		// 1) we pass the arguments from the command line
		// 2) we won't use the main loop at first, we'll start it later
		//    (that's the "false" as second argument)
		// 3) the callbacks are implemented by the TextConsole class above
		Rengine re=new Rengine(new String[] { "--vanilla" }, false, null);
        System.out.println("Rengine created, waiting for R");
		// the engine creates R is a new thread, so we should wait until it's ready
        if (!re.waitForR()) {
            System.out.println("Cannot load R");
            return null;
        }
        //load emd library
      	re.eval("library(EMD)");
        //load signal library for interp1
      	re.eval("library(signal)");
      	
        MyR.re = re;
        return re;
	}
	
	protected void finalize(){
		if (re != null){
			re.end();
		    MP.println("MyR finaliaze: r engine end");
		    MP.println("");
		}
	}
	
}
