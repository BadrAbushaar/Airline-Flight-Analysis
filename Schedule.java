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

public class Schedule {

    public static class ScheduleMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final int delayThreshold = 15;
            String line = value.toString();
            String[] parts = line.split(",");
            String year = parts[0];
            String carrier = parts[8];
            String delayArrival = parts[14];
            String delayDeparture = parts[15];

            // Check if the input line is valid
            if (!year.equals("NA") &&
                    !carrier.equals("NA") &&
                    !delayArrival.equals("NA") &&
                    !delayDeparture.equals("NA") &&
                    !year.equals("Year") &&
                    !carrier.equals("UniqueCarrier") &&
                    !delayArrival.equals("ArrDelay") &&
                    !delayDeparture.equals("DepDelay")) {

                // Check if the sum of arrival and departure delays is less than the threshold
                if (Integer.parseInt(delayArrival) + Integer.parseInt(delayDeparture) <= delayThreshold) {
                    context.write(new Text(carrier), new IntWritable(1)); // Less than threshold
                } else {
                    context.write(new Text(carrier), new IntWritable(0)); // More than threshold
                }
            }

        }
    }

    public static class ScheduleReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Schedule <input folder> <output path> <start year> <num years>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Schedule");
        job.setJarByClass(Schedule.class);
        job.setMapperClass(ScheduleMapper.class);
        job.setReducerClass(ScheduleReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (args.length < 4) {
            System.out.println("Required Parameters:\n - [0]: Input Folder\n - [1]: Output Folder\n - Amount of Years to process");
        }

        String inputFolder = args[0];
        String outputPath = args[1];
        int startYear = 1987 ;
        int numYears = Integer.parseInt(args[2]);

        for (int i = 0; i < numYears; i++) {
            int year = startYear + i;
            String filePath = inputFolder + "/" + year + ".csv";
            FileInputFormat.addInputPath(job, new Path(filePath));
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}