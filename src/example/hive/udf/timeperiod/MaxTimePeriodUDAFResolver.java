package example.hive.udf.timeperiod;

import java.math.BigInteger;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.joda.time.Interval;


public class MaxTimePeriodUDAFResolver extends BaseTimePeriodUDAFResolver {
	private static final Log LOG = LogFactory.getLog(MaxTimePeriodUDAFResolver.class.getName());
	
	@Override
	protected GenericUDAFEvaluator getEvaluator() {
		return new MaxAggTimePeriodEvaluator();
	}

	
	public class MaxAggTimePeriodEvaluator extends BaseTimePeriodEvaluator {
		
		@Override
		public ObjectInspector finalOutputInspector() {
			return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		}
		
		@SuppressWarnings("deprecation")
		@Override
	    public Object terminate(AggregationBuffer agg) throws HiveException {
			BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
			DoubleWritable result = new DoubleWritable();
			double rs = -1.0;
			if (myagg == null) return rs;
			List<Interval> list = sortAndHandleOverlapTimeline(myagg.getTimeLongList());
			if(list.size() > 0) {
				BigInteger maxMillseconds = BigInteger.valueOf(list.get(0).getEnd().getMillis() - list.get(0).getStart().getMillis());
				for (int i = 1; i<list.size(); i++) {
					BigInteger current = BigInteger.valueOf(list.get(i).getEnd().getMillis() - list.get(i).getStart().getMillis());
					maxMillseconds = maxMillseconds.max(current);
				}
				rs = convertToSpecificUnit(maxMillseconds, agg);
			} else {
				LOG.warn("There is no any valid data");
			}
			result.set(rs);
			return result;
		}
	}
}
