<div class="twikiToc"> <ul>
<li> <a href="?_source=newWiki#Hadoop对文本文件的快速全局排序"> Hadoop对文本文件的快速全局排序</a>
</li> <li> <a href="?_source=newWiki#一、背景"> 一、背景</a>
</li> <li> <a href="?_source=newWiki#二、功能"> 二、功能</a>
</li> <li> <a href="?_source=newWiki#三、下载、使用"> 三、下载、使用</a>
</li> <li> <a href="?_source=newWiki#四、性能"> 四、性能</a>
</li> <li> <a href="?_source=newWiki#五、实现"> 五、实现</a>
</li></ul> 
</div>
<h2><a name="一、背景"></a> 一、背景 </h2>
<p />
<span style="background-color: transparent;">Hadoop中实现了用于全局排序的InputSampler类和TotalOrderPartitioner类，调用示例是org.apache.hadoop.examples.Sort。</span>
<p />
但是当我们以Text文件作为输入时，结果并非按Text中的string列排序，而且输出结果是SequenceFile。
<p />
原因：
<p />
<span style="background-color: transparent;">1） hadoop在处理Text文件时，key是行号LongWritable类型，InputSampler抽样的是key，TotalOrderPartitioner也是用key去查找分区。这样，抽样得到的partition文件是对行号的抽样，结果自然是根据行号来排序。</span>
<p />
<span style="background-color: transparent;"> </span><span style="background-color: transparent;">2）大数据量时，InputSampler抽样速度会非常慢。比如，RandomSampler需要遍历所有数据，IntervalSampler需要遍历文件数与splits数一样。SplitSampler效率比较高，但它只抽取每个文件前面的记录，不适合应用于文件内有序的情况。</span>
<h2><a name="二、功能"></a> 二、功能 </h2>
<p />
1. 实现了一种局部抽样方法PartialSampler，适用于输入数据各文件是独立同分布的情况
<p />
2. 使RandomSampler、IntervalSampler、SplitSampler支持对文本的抽样
<p />
3. 实现了针对Text文件string列的TotalOrderPartitioner
<h2><a name="三、下载、使用"></a> 三、下载、使用 </h2>
<p />
<p />
usage：
<p />
hadoop jar yitengfei.jar com.yitengfei.Sort <span style="background-color: transparent;">[-m &lt;maps&gt;] [-r &lt;reduces&gt;] </span>
<p />
<span style="background-color: transparent;">[-splitRandom &lt;double pcnt&gt; &lt;numSamples&gt; &lt;maxsplits&gt; | // Sample from random splits at random (general)</span>
<p />
-splitSample &lt;numSamples&gt; &lt;maxsplits&gt; | // Sample from first records in splits (random data)
<p />
-splitInterval &lt;double pcnt&gt; &lt;maxsplits&gt;] // Sample from splits at intervals (sorted data)
<p />
-splitPartial &lt;double pcnt&gt; &lt;numSamples&gt; &lt;maxsplits&gt; | // Sample from partial splits at random (general) ]
<p />
&lt;input&gt; &lt;output&gt; &lt;partitionfile&gt;
<p />
Example:
<p />
hadoop jar yitengfei.jar com.yitengfei.Sort -r 10 -splitPartial 0.1 10000 10 /user/rp-rd/yitengfei/sample/input /user/rp-rd/yitengfei/sample/output /user/rp-rd/yitengfei/sample/partition
<h2><a name="四、性能"></a> 四、性能 </h2>
<p />
200G输入数据，15亿条url，排序时间只用了6分钟
<h2><a name="五、实现"></a> 五、实现 </h2>
<p />
<strong>1. <span class="twikiNewLink"><a href="/twiki/bin/edit/Ps/RP/PartialSampler?topicparent=Ps/RP.HadoopTotalOrderPartitioner;nowysiwyg=0" rel="nofollow" title="PartialSampler (this topic does not yet exist; you can create it)">PartialSampler</a></span></strong>
<p />
从第一份输入数据中随机抽取第一列文本数据。
<pre><div id="_mcePaste">public K[] getSample(InputFormat&lt;K,V&gt; inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList&lt;K&gt; samples = new ArrayList&lt;K&gt;(numSamples);
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);      
      // 对splits【0】抽样
      for (int i = 0; i &lt; 1; i++) {
       System.out.println("PartialSampler will getSample splits["+i+"]");
        RecordReader&lt;K,V&gt; reader = inf.getRecordReader(splits[i], job,
            Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          if (r.nextDouble() &lt;= freq) {
            if (samples.size() &lt; numSamples) {
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
    }</div></pre>
<p />
首先通过InputFormat的getSplits方法得到所有的输入分区；<span style="background-color: transparent;">然后扫描第一个分区中的记录进行采样。</span>
<p />
记录采样的具体过程如下：
<p />
从指定分区中取出一条记录，判断得到的随机浮点数是否小于等于采样频率freq
<p />
<span style="white-space: pre;">1 </span>如果大于则放弃这条记录；
<p />
<span style="background-color: transparent;">2 如果小于，则判断当前的采样数是否小于最大采样数，</span>
<p />
<span style="white-space: pre;">2.1 </span>如果小于则这条记录被选中，被放进采样集合中；
<p />
<span style="white-space: pre;">2.2 </span>否则从【0，numSamples】中选择一个随机数，如果这个随机数不等于最大采样数numSamples，则用这条记录替换掉采样集合随机数对应位置的记录，同时采样频率freq减小变为freq*(numSamples-1)/numSamples。
<p />
然后依次遍历分区中的其它记录。
<p />
note：
<p />
1）PartialSampler只适用于输入数据各文件是独立同分布的情况。
<p />
2）自带的三种Sampler通过修改getSample函数也可以实现对第一列的抽样。
<p />
<strong>2. <span class="twikiNewLink"><a href="/twiki/bin/edit/Ps/RP/TotalOrderPartitioner?topicparent=Ps/RP.HadoopTotalOrderPartitioner;nowysiwyg=0" rel="nofollow" title="TotalOrderPartitioner (this topic does not yet exist; you can create it)">TotalOrderPartitioner</a></span></strong>
<p />
<span class="twikiNewLink"><a href="/twiki/bin/edit/Ps/RP/TotalOrderPartitioner?topicparent=Ps/RP.HadoopTotalOrderPartitioner;nowysiwyg=0" rel="nofollow" title="TotalOrderPartitioner (this topic does not yet exist; you can create it)">TotalOrderPartitioner</a></span> 主要改进了两点：
<p />
1）读partition时指定keyClass为Text.class
<p />
2）查找分区时，改用value查
<p />
<strong>3. Sort</strong>
<p />
1）设置InputFormat、OutputFormat、OutputKeyClass、OutputValueClass、MapOutputKeyClass，根据输入参数配置任务
<p />
2）初始化InputSampler对象，执行抽样
<p />
3）partitionFile通过CacheFile传给TotalOrderPartitioner，执行MapReduce任务
<div><pre style="margin-top: 0px; margin-bottom: 0px; margin-left: 22px; white-space: pre-wrap; word-wrap: break-word; font-size: 12px; color: #000000; line-height: 18px; font-family: 'Courier New' !important;">
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
      InputSampler.&lt;K,V&gt;writePartitionFile(jobConf, sampler);
      
      URI partitionUri = new URI(partitionFile.toString() + "#" + "_sortPartitioning");
      DistributedCache.addCacheFile(partitionUri, jobConf);
      DistributedCache.createSymlink(jobConf);
    }

    FileSystem hdfs = FileSystem.get(jobConf);
    hdfs.delete(outputpath);
    hdfs.close();
    
    jobResult = JobClient.runJob(jobConf);</pre> </div>
