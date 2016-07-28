# GlobalSort
Hadoop对文本文件的快速全局排序<br>
一、背景<br>
Hadoop中实现了用于全局排序的InputSampler类和TotalOrderPartitioner类，调用示例是org.apache.hadoop.examples.Sort。<br>
但是当我们以Text文件作为输入时，结果并非按Text中的string列排序，而且输出结果是SequenceFile。<br>
原因：<br>
1） hadoop在处理Text文件时，key是行号LongWritable类型，InputSampler抽样的是key，TotalOrderPartitioner也是用key去查找分区。这样，抽样得到的partition文件是对行号的抽样，结果自然是根据行号来排序。<br>
2）大数据量时，InputSampler抽样速度会非常慢。比如，RandomSampler需要遍历所有数据，IntervalSampler需要遍历文件数与splits数一样。SplitSampler效率比较高，但它只抽取每个文件前面的记录，不适合应用于文件内有序的情况。<br>

二、功能<br>
1. 实现了一种局部抽样方法PartialSampler，适用于输入数据各文件是独立同分布的情况<br>
2. 使RandomSampler、IntervalSampler、SplitSampler支持对文本的抽样<br>
3. 实现了针对Text文件string列的TotalOrderPartitioner<br>

三、使用<br>
usage：<br>
hadoop jar yitengfei.jar com.yitengfei.Sort [-m <maps>] [-r <reduces>]<br>
[-splitRandom <double pcnt> <numSamples> <maxsplits> | // Sample from random splits at random (general)<br>
-splitSample <numSamples> <maxsplits> | // Sample from first records in splits (random data)<br>
-splitInterval <double pcnt> <maxsplits>] // Sample from splits at intervals (sorted data)<br>
-splitPartial <double pcnt> <numSamples> <maxsplits> | // Sample from partial splits at random (general) ]<br>
<input> <output> <partitionfile><br>
Example:<br>
hadoop jar yitengfei.jar com.yitengfei.Sort -r 10 -splitPartial 0.1 10000 10 /user/rp-rd/yitengfei/sample/input<br> /user/rp-rd/yitengfei/sample/output /user/rp-rd/yitengfei/sample/partition<br>

四、性能<br>
200G输入数据，15亿条url，排序时间只用了6分钟<br>

五、实现<br>
1. PartialSampler<br>
从第一份输入数据中随机抽取第一列文本数据。<br>
public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {<br>
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());<br>
      ArrayList<K> samples = new ArrayList<K>(numSamples);<br>
      Random r = new Random();<br>
      long seed = r.nextLong();<br>
      r.setSeed(seed);<br>
      LOG.debug("seed: " + seed);      
      // 对splits【0】抽样
      for (int i = 0; i < 1; i++) {
       System.out.println("PartialSampler will getSample splits["+i+"]");
        RecordReader<K,V> reader = inf.getRecordReader(splits[i], job,
            Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          if (r.nextDouble() <= freq) {
            if (samples.size() < numSamples) {
               // 选择value中的第一列抽样
               Text value0 = new Text(value.toString().split("\t")[0]);         
               samples.add((K) value0);               
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one, then adjust the frequency
              // to reflect the possibility of existing elements being
              // pushed out
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
               Text value0 = new Text(value.toString().split("\t")[0]);  
                samples.set(ind, (K) value0);
              }
              freq *= (numSamples - 1) / (double) numSamples;
            }
            key = reader.createKey();
          }
        }        
        reader.close();
      }
      return (K[])samples.toArray();
    }
首先通过InputFormat的getSplits方法得到所有的输入分区；然后扫描第一个分区中的记录进行采样。
记录采样的具体过程如下：
从指定分区中取出一条记录，判断得到的随机浮点数是否小于等于采样频率freq
1 如果大于则放弃这条记录；
2 如果小于，则判断当前的采样数是否小于最大采样数，
2.1 如果小于则这条记录被选中，被放进采样集合中；
2.2 否则从【0，numSamples】中选择一个随机数，如果这个随机数不等于最大采样数numSamples，则用这条记录替换掉采样集合随机数对应位置的记录，同时采样频率freq减小变为freq*(numSamples-1)/numSamples。
然后依次遍历分区中的其它记录。
note：
1）PartialSampler只适用于输入数据各文件是独立同分布的情况。
2）自带的三种Sampler通过修改getSample函数也可以实现对第一列的抽样。

2. TotalOrderPartitioner
TotalOrderPartitioner 主要改进了两点：
1）读partition时指定keyClass为Text.class
2）查找分区时，改用value查

3. Sort
1）设置InputFormat、OutputFormat、OutputKeyClass、OutputValueClass、MapOutputKeyClass，根据输入参数配置任务
2）初始化InputSampler对象，执行抽样
3）partitionFile通过CacheFile传给TotalOrderPartitioner，执行MapReduce任务
    Class inputFormatClass = TextInputFormat.class;
    Class outputFormatClass = TextOutputFormat.class;
    Class outputKeyClass = Text.class;
    Class outputValueClass = Text.class;

    jobConf.setMapOutputKeyClass(LongWritable.class);
    jobConf.setNumReduceTasks(num_reduces);
    
    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);

    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);
    if (sampler != null ) {
      System.out.println("Sampling input to effect total-order sort...");
      jobConf.setPartitionerClass(TotalOrderPartitioner.class);
      Path inputDir = FileInputFormat.getInputPaths(jobConf)[0];
      inputDir = inputDir.makeQualified(inputDir.getFileSystem(jobConf));
      TotalOrderPartitioner.setPartitionFile(jobConf, partitionFile);
      InputSampler.<K,V>writePartitionFile(jobConf, sampler);
      
      URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
      DistributedCache.addCacheFile(partitionUri, jobConf);
      DistributedCache.createSymlink(jobConf);
    }

    FileSystem hdfs = FileSystem.get(jobConf);
    hdfs.delete(outputpath);
    hdfs.close();
    
    jobResult = JobClient.runJob(jobConf);
