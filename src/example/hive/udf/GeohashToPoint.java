package example.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import example.hive.udf.util.GeoHashUtils;

public class GeohashToPoint extends GenericUDTF {
	private static final Integer OUT_COLS = 2;
    //the output columns size
    private transient Object forwardColObj[] = new Object[OUT_COLS];
	private transient ObjectInspector[] inputOIs;
	
	@Override
    public void close() throws HiveException {
	}
	
	@Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {
        if (argOIs.length != 1 || argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                || !argOIs[0].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
            throw new UDFArgumentException("only take one argument with type of string");
        }
        inputOIs = argOIs;
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("latitude");
        fieldNames.add("longtitude");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String input = ((StringObjectInspector)inputOIs[0]).getPrimitiveJavaObject(args[0]);
        double[] decodeGeoHash = GeoHashUtils.decode(input);
        //System.out.println("input="+result);
        forwardColObj[0] = decodeGeoHash[0];
        forwardColObj[1] = decodeGeoHash[1];
        forward(forwardColObj);
    }
}
