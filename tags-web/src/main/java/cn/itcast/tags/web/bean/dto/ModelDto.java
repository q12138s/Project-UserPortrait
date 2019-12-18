package cn.itcast.tags.web.bean.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class ModelDto {
    private Long id;
    private String name;
    private String mainClass;
    private String path;
    private String args;
    //调用频次
    private Schedule schedule;
    private Integer state;
    @Data
    public static class Schedule {
        private Integer frequency;
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm")
        private Date startTime;
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm")
        private Date endTime;
        public String toPattern(){
            String schedule = "";
            String starStr = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(startTime);
            String endStr = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(endTime);
            return frequency + "," + starStr + "," + endStr;
        }
        public static String formatTime(Date time) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'+0800'");
            return formatter.format(time);
        }
    }
    public Schedule parseDate(String sourceDate){
        Schedule schedule = new Schedule();
        try {
            String[] sourceArr = sourceDate.split(",");
            schedule.setFrequency(Integer.parseInt(sourceArr[0]));
            schedule.setStartTime(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(sourceArr[1]));
            schedule.setEndTime(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(sourceArr[2]));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return schedule;
    }
}