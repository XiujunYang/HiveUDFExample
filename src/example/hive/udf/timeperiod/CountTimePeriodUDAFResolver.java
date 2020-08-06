package example.hive.udf.timeperiod;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.joda.time.Interval;


public class CountTimePeriodUDAFResolver extends BaseTimePeriodUDAFResolver {
	private static final Log LOG = LogFactory.getLog(CountTimePeriodUDAFResolver.class.getName());
	
	@Override
	protected GenericUDAFEvaluator getEvaluator() {
		return new CountAggTimePeriodEvaluator();
	}

	
	public class CountAggTimePeriodEvaluator extends BaseTimePeriodEvaluator {
		
		@Override
		public ObjectInspector finalOutputInspector() {
			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		}
		
		@SuppressWarnings("deprecation")
		@Override
	    public Object terminate(AggregationBuffer agg) throws HiveException {
			BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
			IntWritable result = new IntWritable();
			double rs = 0.0;
			if (myagg == null) return rs;
			List<Interval> list = sortAndHandleOverlapTimeline(myagg.getTimeLongList());
			if(list.size() < 1) LOG.warn("There is no any valid data");
			result.set(list.size());
			return result;
		}
	}
}
