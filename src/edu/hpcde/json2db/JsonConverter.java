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
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
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
	private static Logger logger = LoggerFactory.getLogger(HelloWorld.class);
	// private JsonGenerator jsonGenerator = null;
	private ObjectMapper objectMapper;
	private StandardServiceRegistry serviceRegistry;
	private static SessionFactory sessionFactory;
	private static Connection con;	
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
	public void readJson2Map(InputStream in) throws Exception {
		HashMap<String, Object> hashmap = objectMapper.readValue(in,HashMap.class);
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
		//logger.info("connecting");
		String urlString = "jdbc:oracle:thin:@//222.199.193.19:1521/orcl";
		String userString = "crawler";
		String password = "crawler";
		Connection con = DriverManager.getConnection(urlString, userString, password);
		return con;
	}
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
	public void saveWeiboUser(String username, String userurl) throws SQLException{
		String sql = "MERGE INTO weibouser w USING(SELECT ? AS n, ? AS u FROM DUAL) s ON (w.`user_url`=s.u) WHEN NOT MATCHED THEN INSERT (`user_name`,`user_url`) VALUES(s.n,s.u)".replace("`", "\"");
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, username);
		pre.setString(2, userurl);
		pre.executeUpdate();
		closeAll(null,pre,null);
	} 
	public  void saveWeiboPost(String mid, String content, String time, String userurl) throws SQLException{
		String sql = "MERGE INTO weibopost w USING(SELECT ? AS mid, ? AS c, TO_DATE(?,'yyyyMMdd-HHmm') AS t, ? AS u FROM DUAL) s ON (w.`mid`=s.mid) WHEN NOT MATCHED THEN INSERT VALUES(S.mid,s.c,s.t,s.u)".replace("`", "\"");
		PreparedStatement pre = con.prepareStatement(sql);
		pre.setString(1, mid);
		pre.setString(2, content);
		pre.setString(3, time);
		pre.setString(4, userurl);
		pre.executeUpdate();
		closeAll(null,pre,null);
	}
	
	public void save2DB(String filepath) throws Exception{
			
		File file = new File(filepath);
		InputStream in = new FileInputStream(file);
		HashMap<String, Object> hashmap = objectMapper.readValue(in,HashMap.class);
		//logger.info("saving: "+hashmap.get("mid").toString());
		ArrayList repostlist = (ArrayList) hashmap.get("repost_list");
		logger.info(repostlist.get(0).toString());
		//saveWeiboUser(hashmap.get("user_sname").toString(), hashmap.get("user_url").toString());
		//saveWeiboPost(hashmap.get("mid").toString(), hashmap.get("content").toString(),hashmap.get("time").toString(),hashmap.get("userurl").toString());
		
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// File file = new File("E:/fworkspace/Ah8KZjeYu.json");
		// InputStream in = new FileInputStream(file);
		// JsonConverter j = new JsonConverter();
		// j.readJson2Map(in);
		JsonConverter jc = new JsonConverter();
		jc.save2DB("E:/fworkspace/Ah8KZjeYu.json");
		jc.destory();
	}

}
