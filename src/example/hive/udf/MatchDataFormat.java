package example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * refer to https://docs.microsoft.com/bs-latn-ba/azure/hdinsight/hadoop/apache-hadoop-hive-java-udf
 * @author xiujun
 *
 */
public class MatchDataFormat extends UDF {

	public String evaluate(String regex, String input, String def)  {
		if (input.trim().matches(regex)) return input.trim();
		else return def;
	}

}
