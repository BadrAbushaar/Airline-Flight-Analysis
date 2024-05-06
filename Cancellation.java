import java.io.IOException;
import java.util.*;

import javax.naming.Context;

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
import java.io.File;

public class Cancellation {

    public static class CancellationMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            String cancel = parts[21];
            String cancellationCode = parts[22];

            // Output the cancellation code if the flight was cancelled
            if (cancel.equals("1") &&
                    !cancel.equals("Cancelled") &&
                    !cancellationCode.equals("NA") &&
                    !cancellationCode.equals("CancellationCode") &&
                    !cancellationCode.isEmpty()) {

                context.write(new Text(cancellationCode), new IntWritable(1)); // Output the cancellation code
            }
        }

    }

    public static class CancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        ArrayList<CancellationCode> cancellationCodes = new ArrayList<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int totalFlights = 0;

            // Count the number of cancelled flights
            for (IntWritable val : values) {
                totalFlights += val.get();
            }

            cancellationCodes.add(new CancellationCode(key.toString(), totalFlights));
        }

        class CancellationCode {
            public String code;
            public int count;

            public CancellationCode(String code, int count) {
                this.code = code;
                this.count = count;
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the most common cancellation code
            int maxCount = 0;
            Text mostCommonCode = new Text();

            for (CancellationCode code : cancellationCodes) {
                if (code.count > maxCount) {
                    maxCount = code.count;
                    mostCommonCode.set(code.code);
                }
            }

            // Output the most common cancellation code
            context.write(mostCommonCode, new IntWritable(maxCount));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Schedule <input folder> <output path> <num years>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cancellation");
        job.setJarByClass(Cancellation.class);
        job.setMapperClass(CancellationMapper.class);
        job.setReducerClass(CancellationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String inputFolder = args[0];
        String outputPath = args[1];
        int startYear = 1987;
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
