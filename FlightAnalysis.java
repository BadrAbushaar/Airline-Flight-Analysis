import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightAnalysis {

    public static class ScheduleMapper extends Mapper<Object, Text, Text, IntWritable> {

    }

    public static class ScheduleReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    }

    public static class TaxiTimeMapper extends Mapper<Object, Text, Text, IntWritable> {

    }

    public static class TaxiTimeReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    }

    public static class CancellationMapper extends Mapper<Object, Text, Text, IntWritable> {

    }

    public static class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    }
}