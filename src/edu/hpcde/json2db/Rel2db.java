package edu.hpcde.json2db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Rel2db extends DatabaseOperator{
	protected static Logger logger = LoggerFactory.getLogger(Rel2db.class);
	protected Connection con;
	protected Jedis jedis;
	protected JedisPool pool;
	public Rel2db() throws Exception{
		this.con = getConnection();
		//JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
		//this.jedis = pool.getResource();
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
	
	@Override
	public void destory() {
		// TODO Auto-generated method stub
		closeAll(null,null,con);
		this.pool.returnResource(this.jedis);
		this.pool.destroy();
	}
	
	public String getFromUrl(String substr,String mid,String user_url) throws SQLException{
		substr = substr.split("赞")[0].replaceAll("\\s","");
		logger.info("substr: "+substr);
		String sql = "select `repost_id`,`repoststring`,`user_url` from weiborepost where `mid`=? and instr(`repoststring`,?)=1".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, mid);
		ps.setString(2, substr+"%");
		ResultSet rs = ps.executeQuery();
		//取得url
		if(rs.next()){
			user_url = rs.getString(3);
		}
		closeAll(rs,ps,null);
		return user_url;
		
	}
	public void writeOneLine(PrintWriter out,String from_url,String cur_url){
		out.println(from_url+","+cur_url);
	}
	public void write2db(String from_url,String cur_url){
		//cur_url following from_url
		//from_url follower cur_url
		jedis.sadd("relationship:"+cur_url+".following", from_url);
		jedis.sadd("relationship:"+from_url+".follower", cur_url);
		
		
	}
	public static void main(String[] args) throws Exception{
		Rel2db rd = new Rel2db();
		String mid = "ABFMmAs8r";
		String sql = "select `mid`,`user_url` from weibopost where `mid`=? ".replace("`", "\"");
		PreparedStatement ps = rd.con.prepareStatement(sql);
		ps.setString(1, mid);
		ResultSet rs = ps.executeQuery();
		String user_url = null;
		if(rs.next()){
			user_url = rs.getString(2);//取得当前分析的微博的用户
		}
		closeAll(rs,ps,null);
		if(user_url==null){
			logger.error("null user!");
			System.exit(1);
		}
		
		sql = "select `repost_id`,`repoststring`,`user_url` from weiborepost where `mid`=? ".replace("`", "\"");
		PreparedStatement ps2 = rd.con.prepareStatement(sql);
		ps2.setString(1, mid);
		ResultSet rs2 = ps2.executeQuery();
		int a = 0;
		PrintWriter out=new PrintWriter(new BufferedWriter(new FileWriter("e:\\wtf.csv", true)));
		while(rs2.next()){
			a++;
			logger.info("循环到第	"+Integer.toString(a)+"	次");
			String repost_string = rs2.getString(2);
			String cur_url = rs2.getString(3);
			String[] sp = repost_string.split("//@");
			if(sp.length >1){
				//有//@则进行处理取得from_url,取不到则用user_url
				String from_url = rd.getFromUrl(sp[1],mid,user_url) ;//取得来源用户
				rd.writeOneLine(out, from_url, cur_url);
				//rd.write2db(from_url,cur_url);
				//卧槽这部分怎么写的？
			}else{
				//明显没有 //@ 则从user_url到cur_url
				rd.writeOneLine(out, user_url, cur_url);
				//rd.write2db(user_url,cur_url);
			}
		}
		closeAll(rs2,ps2,null);
		out.close();
	}
}
