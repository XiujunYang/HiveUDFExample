package example.hive.udf;

import java.util.Properties;

import org.python.core.PyFloat;
import org.python.core.PyFunction;
import org.python.core.PyList;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;
import org.apache.hadoop.hive.ql.exec.UDF;

public class ExtractGeoLocation extends UDF {
	
	private static PythonInterpreter interpreter = null;
	
	private static void init() {
		Properties props = new Properties();
		props.put("python.console.encoding", "UTF-8");
		props.put("python.security.respectJavaAccessibility", "false");
		props.put("python.import.site", "false");
		props.put("python.path", "py_module"); // python package under root_project/python_module folder
		Properties preprops = System.getProperties();
		PythonInterpreter.initialize(preprops, props, new String[0]);
		interpreter = new PythonInterpreter();
		interpreter.exec("from convert import *");
	}
	
	
	/**
	 * extract address from location.
	 */
	public String evaluate(double lat, double lon, String language) {
		System.out.println("3 parms:" + String.valueOf(lat) + String.valueOf(lon) + language);
		if(interpreter == null) init();
		PyFunction pf = (PyFunction)interpreter.get("convert_to_address");
        String result = pf.__call__(new PyFloat(lat), new PyFloat(lon), (language == null || language.isEmpty())?new PyString("en"):new PyString(language))
        		.asString();
		if (result != null && !result.isEmpty()) return result;
		else return null;
	}
	
	/**
	 * extract location(format=lat,lon) from address.
	 */
	public String evaluate(String addr, String language) {
		System.out.println("2 parms:" + addr + language);
		if(interpreter == null) init();
		PyFunction pf = (PyFunction)interpreter.get("convert_to_location");
		PyList result = (PyList) pf.__call__(new PyString(addr), (language == null || language.isEmpty())?new PyString("en"):new PyString(language));
		Float arr[] = new Float[2];
	    result.toArray(arr);
	    if (result.size() == 0) return null;
	    else {
	    	return new String(arr[0]+","+arr[1]);
	    	//return arr; //Float[]
	    }
	}
	
	/** 
	 * validate latitude and longitude(format=lat,lon) in specific area, such as country or city.
	 */
	public String evaluate(double lat, double lon, String includedInAddr, String language) {
		if(interpreter == null) init();
		PyFunction pf = (PyFunction)interpreter.get("convert_to_address");
		String convert1 = pf.__call__(new PyFloat(lat), new PyFloat(lon), (language == null || language.isEmpty())?new PyString("en"):new PyString(language))
        		.asString();
		String convert2 = pf.__call__(new PyFloat(lon), new PyFloat(lat), (language == null || language.isEmpty())?new PyString("en"):new PyString(language))
        		.asString();
		if (convert1 != null && !convert1.isEmpty() && convert1.contains(includedInAddr)) {
			return String.valueOf(lat)+","+String.valueOf(lon);
		} else if (convert2 != null && !convert2.isEmpty() && convert2.contains(includedInAddr)) {
			return String.valueOf(lon)+","+String.valueOf(lat);
		} else return null;
	}
	
	
	public static void main(String[] args) {
		String addr = "dubai mall";
		if(interpreter == null) init();
		PyFunction pf = (PyFunction)interpreter.get("convert_to_location");
		PyList result = (PyList) pf.__call__(new PyString(addr), new PyString("en"));
		if(result.size() == 0) System.out.println("size is zero");
		Float arr[] = new Float[2];
	    result.toArray(arr);
		System.out.println("result is "+result);
	
	}
	
}
