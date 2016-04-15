package utils;

/**
 * 
 * @author libingtao
 * for remove ugly System.out :) 
 */
public class MP {
	
	public static boolean DEBUG = true;
	
	public static void debug(){
		DEBUG = true;
	}
	
	public static void closeDebug(){
		DEBUG = false;
	}

	public static void println(Object obj){
		if (DEBUG){
			System.out.println(obj);
		}
	}
	
	public static void println(){
		if (DEBUG){
			System.out.println("");
		}
	}
	
	public static void print(Object obj){
		if (DEBUG){
			System.out.print(obj);
		}
	}
	
}
