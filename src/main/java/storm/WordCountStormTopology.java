package storm;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountStormTopology {
	public static class DataSourceSpout extends BaseRichSpout{
		
		/**
		 * Secondary
		 * recycle method会一直不断被调用
		 */
		@Override
		public void nextTuple() {
			Collection<File> listFiles = FileUtils.listFiles(new File("d:\\test"), new String[]{"txt"}, true);
			for (File file : listFiles) {
				List<String> readLines;
				try {
					readLines = FileUtils.readLines(file);
					for (String line : readLines) {
						this.collector.emit(new Values(line));//emit(List tuple)
					}
					//获取File路径+当前时间戳
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			
		}
		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		/**
		 * Firstly
		 * 初始化的method,只会执行一次，在这里面可以
		 * Map conf：配置类
		 * TopologyContext context:topology上下文
		 * SpoutOutputCollector collector：发射器emit
		 */
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		/**
		 * Thirdly声明输出字段，也就是给发送出去的数据对应起个名字
		 */
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
	}
	public static class SpiltBolt extends BaseRichBolt{

		@Override
		public void execute(Tuple arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			// TODO Auto-generated method stub
			
		}
		
	}
	public static class CountBolt extends BaseRichBolt {
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
	
		@Override
		public void execute(Tuple arg0) {
			
		}
		/**
		 * 初始化的method
		 */
		@Override
		public void prepare(Map stormConf, TopologyContext contxt, OutputCollector collector) {
			this.stormConf=stormConf;
			this.context=context;
			this.collector=collector;
		}
	
		@Override
		public void declareOutputFields(OutputFieldsDeclarer arg0) {
			
		}
	}
}
