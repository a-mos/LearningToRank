import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

public class MainJob extends Configured implements Tool {
    public static class MainJobMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final HashMap<String, Integer> urlsMap = new HashMap<>();
        private final HashMap<String, Integer> queryMap = new HashMap<>();
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        private boolean hostFeaturestype = false;

        private void makeMapFromFile(BufferedReader reader, HashMap<String, Integer> map) throws IOException {
            String line = reader.readLine();
            while (line != null){
                String[] splited = line.trim().split("\t");
                Integer id = Integer.parseInt(splited[0]);
                String url = splited[1];
                url = url.replace("http://","").replace("https://","").replace("www.", "");
                if (url.endsWith("/")) {
                    url = url.substring(0, url.length() - 1);
                }
                map.put(url, id);
                line = reader.readLine();
            }
        }

        private void readTrainSet(BufferedReader reader, HashSet<String> set) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private void readTestSet(BufferedReader reader, HashSet<String> set) throws IOException {
            reader.readLine();
            String line = reader.readLine();
            while(line != null) {
                String[] splited = line.trim().split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        @Override
        protected void setup(Context context) throws IOException {
            hostFeaturestype = context.getConfiguration().get("features_type").equals("host");

            String urlsFileName = context.getConfiguration().get("urls_filename");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader urlsFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(urlsFileName)), StandardCharsets.UTF_8));
            long start = System.currentTimeMillis();
            makeMapFromFile(urlsFileReader, urlsMap);
            long end = System.currentTimeMillis();
            urlsFileReader.close();

            String queriesFileName = context.getConfiguration().get("queries_filename");
            BufferedReader queriesFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(queriesFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            makeMapFromFile(queriesFileReader, queryMap);
            end = System.currentTimeMillis();
            queriesFileReader.close();

            String trainSetFileName = context.getConfiguration().get("train_set_filename");
            BufferedReader trainSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTrainSet(trainSetFileReader, trainSet);
            end = System.currentTimeMillis();
            trainSetFileReader.close();

            String testSetFileName = context.getConfiguration().get("test_set_filename");
            BufferedReader testSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTestSet(testSetFileReader, testSet);
            end = System.currentTimeMillis();
            testSetFileReader.close();
        }

        private StringBuilder makeValue(Session session, int pos, String prefix) {
            StringBuilder res = new StringBuilder();
            res.append(prefix); res.append(" ");
            res.append(pos); res.append(" ");
            res.append(session.minClickPos); res.append(" ");
            int clickIdx = -1;
            int clickPrev = 0;
            int clickNext = 0;
            int clickAbove = 0;
            int clickBelow = 0;
            long timeOnClicked = 0;

            for (int i = 0; i < session.clickedPos.length; i++) {
                if (session.clickedPos[i] == pos - 1) {
                    clickPrev = 1;
                    break;
                }
            }
            for (int i = 0; i < session.clickedPos.length; i++) {
                if (session.clickedPos[i] == pos + 1) {
                    clickNext = 1;
                    break;
                }
            }

            if (clickPrev == 0) {
                for (int i = 0; i < session.clickedPos.length; i++) {
                    if (session.clickedPos[i] < pos) {
                        clickAbove = 1;
                        break;
                    }
                }
            } else {
                clickAbove = 1;
            }

            if (clickNext == 0) {
                for (int i = 0; i < session.clickedPos.length; i++) {
                    if (session.clickedPos[i] > pos) {
                        clickBelow = 1;
                        break;
                    }
                }
            } else {
                clickBelow = 1;
            }

            for (int i = 0; i < session.clickedPos.length; i++) {
                if (session.clickedPos[i] == pos) {
                    clickIdx = i;
                    if (session.clickedTimestamps.length > i + 1) {
                        // System.out.println("???????????????????????????????");
                        // System.out.println(Long.toString(session.clickedPos.length));
                        // System.out.println(Long.toString(session.clickedTimestamps.length));
                        // System.out.println(Long.toString(i));
                        // System.out.println(Long.toString(pos));
                        // System.out.println(Long.toString(session.clickedPos[i]));
                        // System.out.println("???????????????????????????????");
                        timeOnClicked = session.clickedTimestamps[i + 1] - session.clickedTimestamps[i];
                    }
                    break;
                }
            }

            if (clickIdx == -1) {
                res.append(0);
            } else {
                res.append(1);
            }
            res.append(" ");
            res.append(clickAbove);
            res.append(" ");
            res.append(clickBelow);
            res.append(" ");
            res.append(clickPrev);
            res.append(" ");
            res.append(clickNext);
            res.append(" ");
            res.append(timeOnClicked);
            res.append(" ");
            res.append(session.clickedPos[session.clickedPos.length - 1]);
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sessionStr = value.toString();
            String query = new StringTokenizer(new StringTokenizer(sessionStr, "\t").nextToken(), "@").nextToken().trim();
            Session session = new Session(sessionStr);
            if (queryMap.containsKey(query)) {
                int queryId = queryMap.get(query);
                for (int i = 0; i < session.shown.length; i++){
                    String url = session.shown[i];
                    if (hostFeaturestype) {
                        try {
                            url = new URL("http://" + url).getHost();
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        String pair = queryId + " " + urlId;
                        StringBuilder res;
                        if (!trainSet.contains(pair) && !testSet.contains(pair)) {
                            res = makeValue(session, i, "G");
                        } else {
                            res = makeValue(session, i, Integer.toString(queryId));
                        }
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            } else {
                for (int i = 0; i < session.shown.length; i++) {
                    String url = session.shown[i];
                    if (hostFeaturestype) {
                        try {
                            url = new URL("http://" + url).getHost();
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    if (urlsMap.containsKey(url)) {
                        int urlId = urlsMap.get(url);
                        StringBuilder res = makeValue(session, i, "G");
                        context.write(new IntWritable(urlId), new Text(res.toString()));
                    }
                }
            }
        }
    }

    public static class MainJobReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private void readTrainSet(BufferedReader reader, HashSet<String> set) throws IOException {
            String line = reader.readLine();
            while (line != null) {
                String[] splited = line.trim().split("\t");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private void readTestSet(BufferedReader reader, HashSet<String> set) throws IOException {
            reader.readLine();
            String line = reader.readLine();
            while(line != null) {
                String[] splited = line.split(",");
                set.add(splited[0] + " " + splited[1]);
                line = reader.readLine();
            }
        }

        private MultipleOutputs<IntWritable, Text> out;
        private final HashSet<String> trainSet = new HashSet<>();
        private final HashSet<String> testSet = new HashSet<>();
        private boolean hostFeaturestype = false;

        @Override
        protected void setup(Context context) throws IOException {
            System.out.println("Reducer");

            hostFeaturestype = context.getConfiguration().get("features_type").equals("host");

            String trainSetFileName = context.getConfiguration().get("train_set_filename");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader trainSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(trainSetFileName)), StandardCharsets.UTF_8));
            long start = System.currentTimeMillis();
            readTrainSet(trainSetFileReader, trainSet);
            long end = System.currentTimeMillis();
            trainSetFileReader.close();

            String testSetFileName = context.getConfiguration().get("test_set_filename");
            BufferedReader testSetFileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(testSetFileName)), StandardCharsets.UTF_8));
            start = System.currentTimeMillis();
            readTestSet(testSetFileReader, testSet);
            end = System.currentTimeMillis();
            testSetFileReader.close();

            out = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            UpdateFeatures globalStats = new UpdateFeatures();
            HashMap<Integer, UpdateFeatures> queryStats = new HashMap<>();
            for (Text v: values) {
                String s = v.toString();
                boolean glob = false;
                if (s.charAt(0) == 'G') {
                    glob = true;
                }
                String[] splited = s.split(" ");
                int queryId = -1;
                if (!glob) {
                    queryId = Integer.parseInt(splited[0]);
                }
                int pos = Integer.parseInt(splited[1]);
                int minClickPos = Integer.parseInt(splited[2]);
                byte click = Byte.parseByte(splited[3]);
                byte above = Byte.parseByte(splited[4]);
                byte below = Byte.parseByte(splited[5]);
                byte prev = Byte.parseByte(splited[6]);
                byte next = Byte.parseByte(splited[7]);
                long time = Long.parseLong(splited[8]);
                int lastClickPos = Integer.parseInt(splited[9]);

                globalStats.updateAll(click, pos, minClickPos, lastClickPos, above, below, prev, next, time);
                if (!glob) {
                    UpdateFeatures t = queryStats.get(queryId);
                    if (t == null) {
                        t = new UpdateFeatures();
                        t.updateAll(click, pos, minClickPos, lastClickPos, above, below, prev, next, time);
                        queryStats.put(queryId, t);
                    } else {
                        t.updateAll(click, pos, minClickPos, lastClickPos, above, below, prev, next, time);
                    }
                }
            }

            StringBuilder globalRes = globalStats.makeValue(-1);
            out.write("globalFeatures", key, new Text(globalRes.toString()));

            for (Integer queryId: queryStats.keySet()) {
                UpdateFeatures t = queryStats.get(queryId);
                StringBuilder queryRes = t.makeValue(queryId);
                String pair = queryId + " " + key.get();
                if (trainSet.contains(pair) || hostFeaturestype) {
                    out.write("queryTrainFeatures", key, new Text(queryRes.toString()));
                } else if (testSet.contains(pair)) {
                    out.write("queryTestFeatures", key, new Text(queryRes.toString()));
                } else {
                    System.out.println("ERROR");
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }

    private Job getMainJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(MainJob.class);
        job.setJobName(MainJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, "globalFeatures", TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "queryTrainFeatures", TextOutputFormat.class, IntWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "queryTestFeatures", TextOutputFormat.class, IntWritable.class, Text.class);

        job.setMapperClass(MainJobMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MainJobReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getMainJobConf(args[0], args[1]);
        job.getConfiguration().set("urls_filename", args[2]);
        job.getConfiguration().set("queries_filename", args[3]);
        job.getConfiguration().set("train_set_filename", args[4]);
        job.getConfiguration().set("test_set_filename", args[5]);
        job.getConfiguration().set("features_type", args[6]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {
        deleteDirectory(new File(args[1]));
        long start = System.currentTimeMillis();
        int exitCode = ToolRunner.run(new MainJob(), args);
        long end = System.currentTimeMillis();
        System.out.println("Time" + (double) (end - start) / 1000);
        System.exit(exitCode);
    }
}
