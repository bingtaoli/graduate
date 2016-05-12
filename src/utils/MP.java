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

	//打印函数，和DEBUG开关无关，打印不可以关闭
	public static void println(Object obj){
		System.out.println(obj);
	}
	public static void println(){
		System.out.println("");
	}
	public static void print(Object obj){
		System.out.print(obj);
	}
	
	//log函数，可以被DEBUG总开关关闭
	//print为false则不打印
	public static void log(Object obj, boolean print){
		if (DEBUG && print){
			System.out.print(obj);
		}
	}
	public static void log(Object obj){
		log(obj, true);
	}
	public static void logln(Object obj, boolean print){
		if (DEBUG && print){
			System.out.println(obj);
		}
	}
	public static void logln(Object obj){
		logln(obj, true);
	}
	
}
