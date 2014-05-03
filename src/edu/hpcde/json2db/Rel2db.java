package edu.hpcde.json2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Rel2db{
	protected static Logger logger = LoggerFactory.getLogger(Rel2db.class);
	protected Jedis jedis;
	protected JedisPool pool;
	protected ObjectMapper objectMapper;
	public Rel2db() throws Exception{
		this.objectMapper = new ObjectMapper();
		this.pool = new JedisPool(new JedisPoolConfig(), "localhost",6379,100000);
		this.jedis = this.pool.getResource();
	}
	public void destory() {
		// TODO Auto-generated method stub
		this.pool.returnResource(this.jedis);
		this.pool.destroy();
	}
	public void writeOneLine(PrintWriter out,String from_url,String cur_url){
		out.println(from_url+","+cur_url);
	}
	public void write2db(String from_url,String cur_url){
		//cur_url following from_url
		//from_url follower cur_url		
		//logger.info("relationship:"+cur_url+".following");
		this.jedis.sadd("relationship:"+cur_url+".following", from_url);
		this.jedis.sadd("relationship:"+from_url+".follower", cur_url);
	}
	public void saveRelationship(File file) throws Exception{
		InputStream inputStream = new FileInputStream(file);
		HashMap<String, Object> hashmap = objectMapper.readValue(inputStream,HashMap.class);
		logger.info("saving: "+hashmap.get("mid").toString());
		String origin_user =  hashmap.get("user_url").toString();
		ArrayList repostlist = (ArrayList) hashmap.get("repost_list");
		for(Object item : repostlist){
			//logger.info("当前循环次数： "+i);
			HashMap m = (HashMap)item;
			String cur_user_url = m.get("user_url").toString(); 
			Object from_user = m.get("from_user_url");
			String from_user_url = null;
			if(from_user!=null){
				from_user_url = from_user.toString();
			}else{
				from_user_url = origin_user;
			}
			write2db(from_user_url,cur_user_url);
		}
		
	}
	public static void main(String[] args) throws Exception{
	
		if(args.length!=1){
			System.out.println("Usage: pass one folder as arg.");
		}else{
			long begintime = System.currentTimeMillis();
			logger.info("start");
			File dir = new File(args[0]);
			File[] files = dir.listFiles();
			int i = 0;
			for(File file:files){
				//分析
				Rel2db rd = new Rel2db();
				i++;
				logger.info("当前循环次数： "+i);
				rd.saveRelationship(file);
				rd.destory();
			}
			
			logger.info("done");
			long endtime=System.currentTimeMillis();
			long costTime = (endtime - begintime)/1000;
			logger.info("In " + String.valueOf(costTime) + " seconds.");
			
		}
	}
}
