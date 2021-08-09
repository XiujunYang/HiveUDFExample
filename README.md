This is sample code to implement UDF,UDAF,UDTF.

[SETUP]
1. Download python package geopy-1.23.0 ,and rename name put under py_module folder, like HiveUDFExample/py_module/geopy_1_23_0
2. create function and use it:
	```
	DROP FUNCTION temp.convert_loc;
	CREATE FUNCTION temp.convert_loc AS 'example.hive.udf.ExtractGeoLocation' USING JAR 'YOUR_BUILT_JAR_PATH';

	select temp.convert_loc(25.073423, 55.136909,'en');
	select temp.convert_loc('dubai mall', 'en');
	
	DROP FUNCTION temp.list_timeperiod;
	CREATE FUNCTION temp.list_timeperiod AS 'example.hive.udf.timeperiod.ListTimePeriodUDAFResolver' USING JAR 's3a://PATH_TO_JAR';
	select id, temp.list_timeperiod(start_time, end_time, 'minute', 3) as union_unixtime from temp.test_timestamp group by id;
	```
