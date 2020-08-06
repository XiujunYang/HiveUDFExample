package example.hive.udf.timeperiod;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.joda.time.Interval;


public class ListTimePeriodUDAFResolver extends BaseTimePeriodUDAFResolver {
	private static final Log LOG = LogFactory.getLog(ListTimePeriodUDAFResolver.class.getName());
	
	@Override
	protected GenericUDAFEvaluator getEvaluator() {
		return new SumAggTimePeriodEvaluator();
	}

	
	public class SumAggTimePeriodEvaluator extends BaseTimePeriodEvaluator {
		
		@Override
		public ObjectInspector finalOutputInspector() {
			return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector);
		}
		
		@SuppressWarnings("deprecation")
		@Override
	    public Object terminate(AggregationBuffer agg) throws HiveException {
			BaseTimePeriodAgg myagg = (BaseTimePeriodAgg) agg;
			List<Timestamp> result = new ArrayList<Timestamp>();
			if (myagg == null) return result;
			List<Interval> list = sortAndHandleOverlapTimeline(myagg.getTimeLongList());
			if(list.size() < 1) LOG.warn("There is no any valid data");
			else {
				list.stream().forEachOrdered(interval -> {
					result.add(new Timestamp(interval.getStartMillis()));
					result.add(new Timestamp(interval.getEndMillis()));
				});
			}
			return result;
		}
	}
}
