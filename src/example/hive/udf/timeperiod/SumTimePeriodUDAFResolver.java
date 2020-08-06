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


public class SumTimePeriodUDAFResolver extends BaseTimePeriodUDAFResolver {
	private static final Log LOG = LogFactory.getLog(SumTimePeriodUDAFResolver.class.getName());
	
	@Override
	protected GenericUDAFEvaluator getEvaluator() {
		return new SumAggTimePeriodEvaluator();
	}

	
	public class SumAggTimePeriodEvaluator extends BaseTimePeriodEvaluator {
		
		@Override
		public ObjectInspector finalOutputInspector() {
			return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		}
		
		@SuppressWarnings("deprecation")
		@Override
	    public Object terminate(AggregationBuffer agg) throws HiveException {
			BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
			DoubleWritable result = new DoubleWritable();
			BigInteger totalMillseconds = BigInteger.valueOf(0);
			double rs = 0.0;
			if (myagg == null) return rs;
			List<Interval> list = sortAndHandleOverlapTimeline(myagg.getTimeLongList());
			if(list.size() < 1) LOG.warn("There is no any valid data");
			for (int i = 0; i<list.size(); i++) {
				totalMillseconds = totalMillseconds.add(BigInteger.valueOf(list.get(i).getEnd().getMillis() - list.get(i).getStart().getMillis()));
			}
			rs = convertToSpecificUnit(totalMillseconds, agg);
			result.set(rs);
			return result;
		}
	}
}
