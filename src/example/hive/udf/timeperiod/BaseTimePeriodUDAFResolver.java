package example.hive.udf.timeperiod;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;


public class BaseTimePeriodUDAFResolver extends AbstractGenericUDAFResolver {
	private static final Log LOG = LogFactory.getLog(BaseTimePeriodUDAFResolver.class.getName());
	protected static String DEFAULT_UNIT = "MILLSECOND";
	protected static int DEFAULT_SCALE = 4;

	@Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		validParameters(parameters);
		return getEvaluator();
	}
	
	protected void validParameters(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length < 2 || parameters.length > 4) {
            throw new UDFArgumentLengthException("Two argument is expected at least, up to four. First and second params will be timestamp such as start and end time; three is unit to convert; four is total scale number expected.");
        }
        
        ObjectInspector[] param = new ObjectInspector[parameters.length];
        for (int i =0; i < parameters.length; i++) {
        	param[i] = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[i]);
        	if(param[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        		throw new UDFArgumentTypeException(i,  "Argument must be PRIMITIVE.");
        	}
        	if(i < 2 && ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP)  {
        		throw new UDFArgumentTypeException(i,"Argument must be Timestamp, but " + ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory().name()+ " was passed.");
        	} else if (i == 2 && ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        		throw new UDFArgumentTypeException(i,"Argument must be String, but "+ ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory().name()+ " was passed.");
        	} else if (i == 3 && ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        		throw new UDFArgumentTypeException(i,"Argument must be Int, but "+ ((PrimitiveObjectInspector) param[i]).getPrimitiveCategory().name()+ " was passed.");
        	}
        }
	}
	
	protected GenericUDAFEvaluator getEvaluator() {
		return new BaseTimePeriodEvaluator();
	}
        
    public static class BaseTimePeriodEvaluator extends GenericUDAFEvaluator {
        PrimitiveObjectInspector inputOI[];
        StructObjectInspector structOI;
        StandardListObjectInspector listOI;
        StructField listField;
        StructField unitField;
        StructField scaleField;
        Object[] partialResult;  
        ListObjectInspector listFieldOI;
		
	static class BaseTimePeriodAgg extends AbstractAggregationBuffer  {
		protected List<LongWritable> timeLongList = new ArrayList<LongWritable>();
		protected String unit;
		protected int scale = -1;
		
		public List<LongWritable> getTimeLongList() {
            return timeLongList;
        }

        public void setTimeLongList(List<LongWritable> list) {
            this.timeLongList = list;
        }
        
        public String getUnit() {
        	return unit;
        }
        
        public void setUnit(String unit) {
        	this.unit = unit;
        }
        
        public int getScale() {
        	return scale;
        }
        
        public void setScale(int scale) {
        	this.scale = scale;
        }
    }
	
	@Override
    public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
		BaseTimePeriodAgg resultAgg = new BaseTimePeriodAgg();
        reset(resultAgg);
        return resultAgg;
    }

	@SuppressWarnings("deprecation")
	@Override
    public void reset(AggregationBuffer agg) throws HiveException {
		BaseTimePeriodAgg resultSumupTotalTimeAgg = (BaseTimePeriodAgg) agg;
		resultSumupTotalTimeAgg.setTimeLongList(new ArrayList<LongWritable>());
		resultSumupTotalTimeAgg.setUnit(null);
    }
	
	@Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m, parameters);
        
        LOG.warn("Mode="+m.name() +"; parameter.len=" + parameters.length +"; parameters[0]=" + parameters.getClass().toString());
        
        //init input
        if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        	assert (parameters.length >= 2);
        	inputOI = new PrimitiveObjectInspector[parameters.length];
        	for(int i = 0; i < parameters.length; i++) {
        		inputOI[i] = (PrimitiveObjectInspector) parameters[i];
        	}
        } else {
        	assert (parameters.length == 1);
        	structOI = (StructObjectInspector) parameters[0];
            listField = structOI.getStructFieldRef("timelongList");
            unitField = structOI.getStructFieldRef("unit");
            scaleField = structOI.getStructFieldRef("scale");
            listFieldOI = (ListObjectInspector) listField.getFieldObjectInspector();
        }
        
        //init output
        if(m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        	listOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        	ArrayList<String> fname = new ArrayList<String>();
        	ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
            fname.add("timelongList");
            fname.add("unit");
            fname.add("scale");
            foi.add(listOI);
            foi.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            foi.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            partialResult = new Object[3];
            return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
        }else {
           return finalOutputInspector();
       }
    }
	
	protected ObjectInspector finalOutputInspector() {
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@SuppressWarnings("deprecation")
	@Override
	public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
		
		if(parameters.length >= 2 && parameters[0] != null && parameters[1] != null) {
			BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
			try {
				Timestamp p1= PrimitiveObjectInspectorUtils.getTimestamp(parameters[0], (PrimitiveObjectInspector)inputOI[0]);
				Timestamp p2 = PrimitiveObjectInspectorUtils.getTimestamp(parameters[1], (PrimitiveObjectInspector)inputOI[1]);
				if(p1.before(p2)) {
					myagg.getTimeLongList().add(new LongWritable(p1.getTime()));
					myagg.getTimeLongList().add(new LongWritable(p2.getTime()));
//					LOG.info("start_time=" + p1 + ";end_time=" + p2 +"; list size=" + myagg.getTimeLongList().size());
				} else {
					LOG.warn("start_time is later than end_time; start_time=" + p1 + " / end_time=" + p2);
				}
			} catch(NullPointerException e) {
				LOG.warn("got a null value, skip it");
				return;
			}
			if(parameters.length >= 3 && parameters[2] != null && myagg.getUnit() == null) {
				String p3 = PrimitiveObjectInspectorUtils.getString(parameters[2], (PrimitiveObjectInspector) inputOI[2]);
//				LOG.info("iterate, set unit: "+p3);
				myagg.setUnit(p3 != null && p3.length() > 0 ? p3 : DEFAULT_UNIT);
			}
			if(parameters.length >= 4 && parameters[3] != null && myagg.getScale() == -1) {
				int p4 = PrimitiveObjectInspectorUtils.getInt(parameters[3], (PrimitiveObjectInspector) inputOI[3]);
//				LOG.info("iterate, set scale: "+p4);
				myagg.setScale(p4 > 0 ? p4  : DEFAULT_SCALE);
			}
		}
		
	}
	
	@SuppressWarnings({"deprecation","unchecked"})
	@Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
        partialResult[0] = new ArrayList<LongWritable>(myagg.getTimeLongList().size());
        partialResult[1] = myagg.getUnit();
        partialResult[2] = myagg.getScale();
        ((ArrayList<LongWritable>) partialResult[0]).addAll(myagg.getTimeLongList());
//        LOG.warn("terminatePartial, intervalList.size="+ ((ArrayList<LongWritable>) partialResult[0]).size() 
//        		+ " ;unit=" + myagg.getUnit() + ";scale="+myagg.getScale());
        return partialResult;
	}
	
	@SuppressWarnings({"deprecation","unchecked"})
	@Override
    public void merge(AggregationBuffer agg, Object partial) {
		BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
        Object partialObject = structOI.getStructFieldData(partial, listField);
        ArrayList<LongWritable> resultList = (ArrayList<LongWritable>) listFieldOI.getList(partialObject);
        Object unit = structOI.getStructFieldData(partial, unitField);
        Object scale = structOI.getStructFieldData(partial, scaleField);
//        LOG.info("merge, resultList.size=" + resultList == null ? "null" : resultList.size() 
//        		+ "; unit=" + ((String)unit) + "; scale=" + (Integer)scale);
        myagg.setUnit((String) unit);
        myagg.setScale((Integer)scale);
        myagg.getTimeLongList().addAll(resultList);
	}
	
	@SuppressWarnings("deprecation")
	@Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
		throw new TaskExecutionException(
				"This is base class, reference to calass [SumTimePeriodUDAFResolver,CountTimePeriodUDAFResolver,FirstTimePeriodUDAFResolver,LastTimePeriodUDAFResolver,MinTimePeriodUDAFResolver,MaxTimePeriodUDAFResolver,ListTimePeriodUDAFResolver]");
	}
	
	protected List<Interval> convertToIntervalList(List<LongWritable> list) {
		List<Interval> result = new ArrayList<Interval>();
		if(list == null || list.size() == 0) return result;
		IntStream.range(0, list.size()).filter(i -> i % 2 == 1).forEach(idx -> {
			result.add(new Interval(list.get(idx-1).get(), list.get(idx).get()));
		});
		return result;
	}
	
	protected List<Interval> sortAndHandleOverlapTimeline(List<LongWritable> list) {
		List<Interval> intervals = convertToIntervalList(list);
		List<Interval> result = new ArrayList<Interval>();

	    if(intervals.size() > 0) {
	    	Collections.sort(intervals, new IntervalComparator());
	        DateTime start = intervals.get(0).getStart();
	        DateTime end = intervals.get(0).getEnd();
	        for (int i=1; i<intervals.size(); i++) {
	        	Interval current = intervals.get(i);
				if (current.getStart().isBefore(end)) {
	                end = current.getEnd().isAfter(end) ? current.getEnd() : end;
	            } else {
	                result.add(new Interval(start, end));
	                start = current.getStart();
	                end = current.getEnd();
	            }
			}
	        result.add(new Interval(start,end));
	    }
        return result;
	}
	
	@SuppressWarnings("deprecation")
	protected double convertToSpecificUnit(BigInteger millseconds, AggregationBuffer agg) {
		BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
		double result = 0.0;
		long divisor = 1;
		switch (myagg.getUnit() == null ? DEFAULT_UNIT : myagg.getUnit().toUpperCase()) {
		case "YEAR":
			divisor = 3600000*24*30*366;
			break;
		case "MONTH":
			divisor = 3600000*24*30;
			break;
		case "DAY":
			divisor = 3600000*24;
			break;
		case "HOUR":
			divisor = 3600000;
			break;
		case "MINUTE":
			divisor = 60000;
			break;
		case "SECOND":
			divisor = 1000;
			break;
		case "MILLSECOND":
		default:
			break;
		}
		result = new BigDecimal(millseconds).divide(new BigDecimal(divisor), 
				myagg.getScale() >= 0 ? myagg.getScale() : DEFAULT_SCALE, BigDecimal.ROUND_HALF_UP).doubleValue();
		return result;
	}
	
	/**
	 * sort by start time then end time.
	 */
	class IntervalComparator implements Comparator<Interval> {
	    public int compare(Interval i1, Interval i2) {
	    	if (!i1.getStart().isEqual(i2.getStart())) return i1.getStart().compareTo(i2.getStart());
	    	return i1.getEnd().compareTo(i2.getEnd());
	    }
	}
    }

}