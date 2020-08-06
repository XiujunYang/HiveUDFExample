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

public class ExplodeMap extends GenericUDTF {
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
            throw new UDFArgumentException("split_url only take one argument with type of string");
        }
        inputOIs = argOIs;
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String result = ((StringObjectInspector)inputOIs[0]).getPrimitiveJavaObject(args[0]);
        //System.out.println("input="+result);
        forwardColObj[0] = result.split(";")[0];
        forwardColObj[1] = result.split(";")[1];
        forward(forwardColObj);
    }
    
}
