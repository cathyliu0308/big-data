import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {
        //Object is key of input(the position or offset of the input file); Text is value of input
        //Text and InWritable is the key-value of output
        public Map<String, String> emotionLibrary = new HashMap<String, String>();
        @Override
        public void setup(Context context) throws IOException{
            //context is the media using for mapreduce interacting with other tools of hadoop
            Configuration configuration = context.getConfiguration();
            String dicName = configuration.get("dictionary", "");
            BufferedReader br = new BufferedReader(new FileReader(dicName));
            String line = br.readLine();

            while (line != null) {
                String[] word_feeling = line.split("\t");
                emotionLibrary.put(word_feeling[0], word_feeling[1]);
                line = br.readLine();
            }
            br.close();

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //read some data
            //split into words
            //look up in sentimentLibrary
            //write out the result key-value (on disk)
            String[] words = value.toString().split("\\s+");
            for (String word:words) {
                if (emotionLibrary.containsKey(word.trim().toLowerCase())) {
                    context.write(new Text(emotionLibrary.get(word.toLowerCase().trim())), new IntWritable(1));
                }
            }
        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //combine data from mapper
            //sum count for each sentiment
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));

        }


    }

    public static void main(String[] args) throws Exception {
        Configuration configuration  = new Configuration();
        configuration.set("dictionary", args[2]);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentSplit.class);
        job.setReducerClass(SentimentCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }


}

