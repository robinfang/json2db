package edu.hpcde.json2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

public class Json2Mongo {
	protected static Logger logger = LoggerFactory.getLogger(JsonConverter.class);
	private ObjectMapper objectMapper;
	private DB db;
	private MongoClient mongoClient;
	public Json2Mongo() throws Exception{
		this.objectMapper = new ObjectMapper();
		this.mongoClient = new MongoClient( "localhost" , 27017 ); 
		this.db = mongoClient.getDB( "test" ); 
		
	}
	public void destory(){
		this.mongoClient.close();
	}
	public void saveUser2DB(File file) throws Exception{
		String fileName=file.getName();
		String prefix=fileName.substring(fileName.lastIndexOf(".")+1);
		if(!prefix.equals("json")) return;
		InputStream inputStream = new FileInputStream(file);
		HashMap<String, Object> hashmap = new HashMap(); 
		hashmap = objectMapper.readValue(inputStream,HashMap.class);
		DBCollection coll = this.db.getCollection("user");
		BasicDBObject keys = new BasicDBObject("url",1);
		BasicDBObject options = new BasicDBObject("unique", "true");
		coll.createIndex(keys, options);
		logger.info("saving: "+hashmap.get("mid").toString());
		//String mid = hashmap.get("mid").toString();
		String name = hashmap.get("user_sname").toString();
		String url = hashmap.get("user_url").toString();
		Map documentMap =new HashMap();
		documentMap.put("url",url);
		documentMap.put("name",name);
		try{
			coll.insert(new BasicDBObject(documentMap));
		}catch(DuplicateKeyException e){
		}
		ArrayList repostlist = (ArrayList) hashmap.get("repost_list");
		for(Object item : repostlist){
			HashMap m = (HashMap)item;
			url = m.get("user_url").toString();
			name = m.get("user_sname").toString();
			documentMap =new HashMap();
			documentMap.put("url",url);
			documentMap.put("name",name);
			try{
				coll.insert(new BasicDBObject(documentMap));
			}catch(DuplicateKeyException e){
				continue;
			}
		}		
		
	}
	
	public static void main(String[] args) throws Exception{
		Json2Mongo jm = new Json2Mongo();
		File dir = new File("E:\\pyworkspace\\weibo_3_20_test");
		File[] files = dir.listFiles();
		for(File file:files){
			jm.saveUser2DB(file);
		}
		jm.destory();
		logger.info("done");
		
	}
}
