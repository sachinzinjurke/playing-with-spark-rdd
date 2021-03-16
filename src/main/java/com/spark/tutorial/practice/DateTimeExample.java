package com.spark.tutorial.practice;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeExample {

    public static void main(String[] args) {

        LocalDate today = LocalDate.now();
        System.out.println("Current Date="+today);

        //Current Time
       /* LocalTime time = LocalTime.now();
        System.out.println("Current Time="+time);
*/
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusDays(1);
        System.out.println("Start Time="+startTime);
        System.out.println("End Time="+endTime);

        while(startTime.isBefore(endTime)){
            startTime = startTime.plusHours(1);
            System.out.println("Incremented startTime ="+startTime  + " new hours : " +startTime.getHour());
            createUrl(startTime);
        }

    }

    private static String createUrl(LocalDateTime startTime) {
        String url="http://localhost//enriched//";


        StringBuffer sb=new StringBuffer(url);
        sb.append(startTime.getYear());
        sb.append("//");
        sb.append(getpaddedValue(String.valueOf(startTime.getMonthValue())));
        sb.append("//");
        sb.append(getpaddedValue(String.valueOf(startTime.getDayOfMonth())));
        sb.append("//");
        sb.append(getpaddedValue(String.valueOf(startTime.getHour())));
        System.out.println(sb);//prints Hello Java
        return sb.toString();
    }

    private static String getpaddedValue(String value) {
        String pattern = "\\d";
        Pattern p = Pattern.compile(pattern);
        if(p.matcher(value).matches()){
            System.out.println("************************************");
            return StringUtils.leftPad(value, 2, '0');
        }else{
            return value;
        }
    }


}
