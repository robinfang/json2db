/**
 * 
 */
package edu.hpcde.json2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fang
 * 
 */
public class JsonConverter {

	/**
	 * @param args
	 */
	protected static Logger logger = LoggerFactory.getLogger(JsonConverter.class);
	// private JsonGenerator jsonGenerator = null;
	protected ObjectMapper objectMapper;
	//private StandardServiceRegistry serviceRegistry;
	//private static SessionFactory sessionFactory;
	protected Connection con;	
	public JsonConverter() throws Exception {
		this.objectMapper = new ObjectMapper();
		this.con = getConnection();
	}
	public void destory(){
		closeAll(null,null,con);
	}
	/**
	 * @param in
	 * @throws Exception
	 */
	public void readJson2Map(InputStream inputStream) throws Exception {
		HashMap<String, Object> hashmap = objectMapper.readValue(inputStream,HashMap.class);
		String content = hashmap.get("content").toString();
		System.out.println(content);
	}
	public static void closeAll(ResultSet rs, PreparedStatement ps,
			Connection con) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
			}
		}
	}
	public static Connection getConnection() throws Exception{
		Class.forName("oracle.jdbc.driver.OracleDriver");
		logger.info("connecting");
		String urlString = "jdbc:oracle:thin:@//222.199.193.19:1521/orcl";
		String userString = "crawler";
		String password = "crawler";
		Connection con = DriverManager.getConnection(urlString, userString, password);
		return con;
	}
	/*
	public void test() {
		Configuration configuration = new Configuration();
		configuration.configure();
		serviceRegistry = new StandardServiceRegistryBuilder().applySettings(
				configuration.getProperties()).build();
		sessionFactory = configuration.buildSessionFactory(serviceRegistry);
		Session session = sessionFactory.openSession();
		Transaction tx = session.beginTransaction();
		// user.setUser_name("demoname3");
		// user.setUser_url("demouserurl3");
		// session.saveOrUpdate(user);
		WeiboUser weibouser = (WeiboUser) session.load(WeiboUser.class,
				"testurl");
		WeiboPost wp = new WeiboPost();
		wp.setContent("democontent");
		wp.setMid("demomid");
		wp.setTime(new Date());
		wp.setWeibouser(weibouser);
		session.save(wp);
		tx.commit();
		sessionFactory.close();
	}
	*/
	/**
	 * @param username
	 * @param userurl
	 * @throws SQLException
	 */
	public void saveWeiboUser(String username, String userurl) throws SQLException{
		String sql = "MERGE INTO weibouser w USING(SELECT ? AS n, ? AS u FROM DUAL) s ON (w.`user_url`=s.u) WHEN NOT MATCHED THEN INSERT (`user_name`,`user_url`) VALUES(s.n,s.u)".replace("`", "\"");
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, username);
		pre.setString(2, userurl);
		pre.executeUpdate();
		closeAll(null,pre,null);
	} 
	/**
	 * @param m
	 * @throws SQLException
	 */
	public void saveWeiboUser(HashMap m) throws SQLException{
		Object o = m.get("user_sname");
		String username;
		if(o!=null){
			username = o.toString();
		}else{
			username = m.get("user_url").toString();
		}
		String user_url = m.get("user_url").toString();
		saveWeiboUser(username, user_url);
	}
	/**
	 * @param mid
	 * @param content
	 * @param time
	 * @param userurl
	 * @return
	 * @throws SQLException
	 */
	public  Boolean saveWeiboPost(String mid, String content, String time, String userurl) throws SQLException{
		String sql = "MERGE INTO weibopost w USING(SELECT ? AS mid, ? AS c, TO_DATE(?,'yyyyMMdd-HH24mi') AS t, ? AS u FROM DUAL) s ON (w.`mid`=s.mid) WHEN NOT MATCHED THEN INSERT VALUES(s.mid,s.c,s.t,s.u)".replace("`", "\"");
		//logger.info(sql);
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, mid);
		pre.setString(2, content);
		pre.setString(3, time);
		pre.setString(4, userurl);
		int affectedRows = pre.executeUpdate();
		closeAll(null,pre,null);
		Boolean saved;
		saved = (affectedRows>0);
		return saved;
	}

	/**
	 * @param time
	 * @param content
	 * @param repoststring
	 * @param user_url
	 * @param mid
	 * @throws SQLException
	 */
	public void saveWeiboRepost(String time, String content, String repoststring, String user_url, String mid) throws SQLException{
		//repostid, time, content, repoststring, user_url, mid
		String sql = "MERGE INTO weiborepost w USING(" +
				"select TO_DATE(?,'yyyyMMdd-HH24mi') AS time, ? AS content, ? AS repoststring, ? AS user_url, ? AS mid FROM DUAL) s " +
				"ON (w.`user_url`=s.user_url and w.`repoststring`=s.repoststring) " +
				"WHEN NOT MATCHED THEN INSERT (`repost_id`,`time`,`content`,`repoststring`,`user_url`,`mid`) " +
				"VALUES(weiborepost_sequence.nextval,s.time,s.content,s.repoststring,s.user_url,s.mid)";
		sql = sql.replace("`", "\"");
		//logger.info(sql);
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, time);
		pre.setString(2, content);
		pre.setString(3, repoststring);
		pre.setString(4, user_url);
		pre.setString(5, mid);
		//logger.info("saved "+repoststring);
		try{
			pre.executeUpdate();
		}catch(SQLException e){
			logger.error("SQLException: "+e);
			logger.error("content: "+content);
		}
		closeAll(null,pre,null);
	}
	/**
	 * @param m
	 * @param user_url
	 * @param mid
	 * @throws SQLException
	 */
	public void saveWeiboRepost(HashMap m, String user_url, String mid) throws SQLException{
		String time = m.get("time").toString();
		String content = m.get("content").toString();
		String repoststring = m.get("repost_string").toString();
		saveWeiboRepost(time, content, repoststring, user_url, mid);
	}
	public void save2DB(File file) throws Exception{
		
		InputStream inputStream = new FileInputStream(file);
		HashMap<String, Object> hashmap = objectMapper.readValue(inputStream,HashMap.class);
		logger.info("saving: "+hashmap.get("mid").toString());
		String mid = hashmap.get("mid").toString();
		saveWeiboUser(hashmap.get("user_sname").toString(), hashmap.get("user_url").toString());
		this.con.setAutoCommit(false);
		Boolean saved = saveWeiboPost(hashmap.get("mid").toString(), hashmap.get("content").toString(),hashmap.get("time").toString(),hashmap.get("user_url").toString());
		if(saved){
			ArrayList repostlist = (ArrayList) hashmap.get("repost_list");
			for(Object item : repostlist){
				HashMap m = (HashMap)item;
				String user_url = m.get("user_url").toString();
				saveWeiboUser(m);
				saveWeiboRepost(m, user_url, mid);
			}
		}else{
			logger.info("mid "+mid+" exists");
		}
		this.con.commit();
	}
	public void save2DB(String filepath) throws Exception{
		File file = new File(filepath);
		save2DB(file);
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// File file = new File("E:/fworkspace/Ah8KZjeYu.json");
		// InputStream in = new FileInputStream(file);
		// JsonConverter j = new JsonConverter();
		// j.readJson2Map(in);
		File dir = new File("E:\\pyworkspace\\fangxiaozhang");
		File[] files = dir.listFiles();
		JsonConverter jc = new JsonConverter();
		for(File file:files){
			jc.save2DB(file);
		}
		//jc.save2DB("e:/workspace/A0ju7ih4D.json");
		jc.destory();
	}

}
