package example.hive.udf;

import java.sql.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Chronology;
import org.joda.time.LocalDate;
import org.joda.time.chrono.IslamicChronology;

public class GregorianConvertToHijrah extends UDF {
	private static final DateTimeZone uaeTimeZone = DateTimeZone.forID("Asia/Dubai");
	
	@SuppressWarnings("deprecation")
	public Date evaluate(Date greLocalDate)  {
		if (greLocalDate == null) return greLocalDate;
		LocalDate hijrah = convertToHijiri(greLocalDate);
//		return new Date(convertToHijiri(greLocalDate).toDateTimeAtStartOfDay(uaeTimeZone).getMillis());
        return new Date(hijrah.getYear()-1900, hijrah.getMonthOfYear()-1, hijrah.getDayOfMonth()); 
	}
	
	// Ramadan will be 9th month of year.
	public boolean evaluate(Date greLocalDate, int monthOfYearInIslamtic) {
		if(greLocalDate == null) return false;
		LocalDate hijrah = convertToHijiri(greLocalDate);
		if(hijrah.getMonthOfYear() == monthOfYearInIslamtic) return true;
		else return false;
	}
	
	public static LocalDate convertToHijiri(Date gregorian) {
		LocalDate greLocalDate = new DateTime(gregorian.getTime()).toLocalDate();
		Chronology hijrah = IslamicChronology.getInstance(uaeTimeZone);
		return new LocalDate(greLocalDate.toDateTimeAtStartOfDay(), hijrah);
	}
}
