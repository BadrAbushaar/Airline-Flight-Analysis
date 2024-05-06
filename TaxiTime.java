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

public class TaxiTime {

    public static class TaxiTimeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            String year = parts[0];
            String originAirport = parts[17];
            String destAirport = parts[18];
            String originTaxi = parts[21];
            String destTaxi = parts[20];

            // Check if the input line is valid
            if (!year.equals("NA") &&
                    !originAirport.equals("Origin") &&
                    !originTaxi.equals("NA") &&
                    !destTaxi.equals("NA")) {

                try {
                    context.write(new Text(originAirport), new DoubleWritable(Double.parseDouble(originTaxi)));
                } catch (NumberFormatException e) {
                    // Continue with no action if parsing fails for originTaxi
                }

                try {
                    context.write(new Text(destAirport), new DoubleWritable(Double.parseDouble(destTaxi)));
                } catch (NumberFormatException e) {
                    // Continue with no action if parsing fails for destTaxi
                }
            }

        }
    }

    public static class TaxiTimeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        List<AvgTaxi> avgTaxis = new ArrayList<AvgTaxi>();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double count = 0;
            double totalTaxi = 0;

            // Count the number of flights and the total taxi time
            for (DoubleWritable val : values) {
                count++;
                totalTaxi += val.get();
            }

            // Calculate the average taxi time
            double onTimeProb = (double) totalTaxi / count;
            context.write(key, new DoubleWritable(onTimeProb));
            avgTaxis.add(new AvgTaxi(key.toString(), onTimeProb));
        }

        // Class to store the carrier and the average taxi time
        class AvgTaxi {
            public String carrier;
            public double avgTaxi;

            public AvgTaxi(String carrier, double avgTaxi) {
                this.carrier = carrier;
                this.avgTaxi = avgTaxi;
            }
        }

        // Comparator to sort the carriers in descending order of average taxi time
        class ReverseSort implements Comparator<AvgTaxi> {
            @Override
            public int compare(AvgTaxi a, AvgTaxi b) {
                return Double.compare(b.avgTaxi, a.avgTaxi);
            }
        }

        // Cleanup method to output the top 3 and bottom 3 carriers by average taxi time
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the carriers in descending order of average taxi time
            Collections.sort(avgTaxis, new ReverseSort());

            for (int i = 0; i < Math.min(avgTaxis.size(), 3); i++) {
                context.write(new Text(avgTaxis.get(i).carrier), new DoubleWritable(avgTaxis.get(i).avgTaxi));
            }
            for (int i = Math.max(0, avgTaxis.size() - 3); i < avgTaxis.size(); i++) {
                context.write(new Text(avgTaxis.get(i).carrier), new DoubleWritable(avgTaxis.get(i).avgTaxi));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Schedule <input folder> <output path> <num years>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaxiTime");
        job.setJarByClass(TaxiTime.class);
        job.setMapperClass(TaxiTimeMapper.class);
        job.setReducerClass(TaxiTimeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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