package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class KVSMapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Logger LOGGER = Logger.getLogger(KVSMapper2.class);
    private static final String TAB = "\t";

    private Cache<String, String> cache;
    private BufferedReader brReader;
    private MultipleOutputs<Text, NullWritable> mos;

    protected void setup(Context context) throws IOException, InterruptedException {

        mos = new MultipleOutputs<Text, NullWritable>((TaskInputOutputContext) context);
        cache = CacheBuilder.newBuilder().maximumSize(1000000).expireAfterWrite(2L, TimeUnit.HOURS).build();
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (Path eachPath : cacheFilesLocal) {
            if (eachPath.getName().trim().equals(UNMAPPED_KEY)) {

                loadMappingHashMap(new Path("/Users/ashish/IdeaProjects/KvsAggregatorWithEncoding/out1/Unmappedkey/-r-00000"));
                //loadMappingHashMap(new Path(eachPath.toString()+"/-r-00000"));
            }
        }
    }

    private void loadMappingHashMap(Path filePath) throws IOException {
        String strLineRead;
        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));
            while ((strLineRead = brReader.readLine()) != null) {
                String mappingValues[] = strLineRead.split(TAB);
                if (mappingValues.length > 1) {
                    cache.put(mappingValues[1], mappingValues[0]);
                }
            }
        } catch (Exception e) {
            LOGGER.info(e);
        } finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    public void map(LongWritable key, Text value, final Context context) throws IOException, InterruptedException {

        final String aggr[] = value.toString().split(TAB);

        if (aggr.length >= 15) {
            String event_type = aggr[4];
            String platform = aggr[5];
            String os = aggr[6];
            String application = aggr[7];
            String bundle_id = aggr[8];
            String kv_id = aggr[9];

            event_type = event_type != null ? event_type.equals("PUT") ? "1" : "0" : null;
            platform = extractFromCache(platform);
            os = extractFromCache(os);
            application = extractFromCache(application);
            bundle_id = extractFromCache(bundle_id);
            kv_id = extractFromCache(kv_id);

            String outTuple = aggr[0] + TAB + aggr[1] + TAB + aggr[2] + TAB
                    + aggr[3] + TAB + event_type + TAB + platform + TAB + os
                    + TAB + application + TAB + bundle_id + TAB + kv_id + TAB
                    + aggr[10] + TAB + aggr[11] + TAB + aggr[12] + TAB
                    + aggr[13] + TAB + aggr[14];

            writePartialEncodedData(context, outTuple);
        }
    }

    private void writePartialEncodedData(final Context context, final String record) throws IOException, InterruptedException {
        mos.write(FULL_ENCODED_DATA, new Text(record), NullWritable.get(), context.getConfiguration().get(FULL_ENCODED_DATA));
    }

    private String extractFromCache(String field) {
        if (field != null) {
            String cacheField = cache.getIfPresent(field);
            if (cacheField != null && field.length() > cacheField.length()) {
                return cacheField;
            }
        }
        return field;
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        mos.close();
    }
}
