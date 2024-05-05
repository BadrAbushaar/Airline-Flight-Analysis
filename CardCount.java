import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CardCount {

  public static class CardMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          // Split the input line into card attributes
          String line = value.toString();
          String[] parts = line.split("-");
          
          // Emit the suit as key and card number as value
          context.write(new Text(parts[1]), new IntWritable(Integer.parseInt(parts[0])));
      }
  }


  public static class CardReducer extends Reducer<Text, IntWritable, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          boolean[] cards = new boolean[15];

          // Mark the present cards
          for (IntWritable val : values) {
              cards[val.get()] = true;
          }

          // Check for missing cards and emit them
          for (int i = 1; i < cards.length; i++) {
              if (!cards[i]) {
                  context.write(key, new Text(i + "-" + key.toString()));
              }
          }
      }
  }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CardDeck");
        job.setJarByClass(CardCount.class);
        job.setMapperClass(CardMapper.class);
        job.setReducerClass(CardReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}