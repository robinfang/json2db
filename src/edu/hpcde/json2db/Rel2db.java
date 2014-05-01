package edu.hpcde.json2db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rel2db extends DatabaseOperator{
	protected static Logger logger = LoggerFactory.getLogger(JsonConverter.class);
	protected Connection con;	
	public Rel2db() throws Exception{
		this.con = getConnection();
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
	}
	
	public String getFromUrl(String substr,String mid,String user_url) throws SQLException{
		substr = substr.split("赞")[0].replaceAll("\\s","");
		logger.info("substr: "+substr);
		String sql = "select `repost_id`,`repoststring`,`user_url` from weiborepost where `mid`=? and `repoststring` like ?".replace("`", "\"");
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
	public void write2db(String from_url,String user_url){
		
	}
	public void main() throws Exception{
		Rel2db rd = new Rel2db();
		String mid = "ABFMmAs8r";
		String sql = "select `mid`,`user_url` from weibopost where `mid`=? ".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, mid);
		ResultSet rs = ps.executeQuery();
		String user_url = null;
		if(rs.next()){
			user_url = rs.getString(2);
		}
		closeAll(rs,ps,null);
		if(user_url==null){
			logger.error("null user!");
			System.exit(1);
		}
		
		sql = "select `repost_id`,`repoststring`,`user_url` from weiborepost where `mid`=? ".replace("`", "\"");
		PreparedStatement ps2 = con.prepareStatement(sql);
		ps2.setString(1, mid);
		ResultSet rs2 = ps.executeQuery();
		while(rs2.next()){
			String repost_string = rs2.getString(2);
			String cur_url = rs2.getString(3);
			String[] sp = repost_string.split("//@");
			if(sp.length >1){
				String from_url = getFromUrl(sp[1],mid,user_url) ;
				if(from_url==null){
					//没取到from_url的话从user_url到cur_url
					write2db(user_url,cur_url);
				}else{
					//取到则从from_url到cur_url
					write2db(from_url,cur_url);
				}
				//卧槽这部分怎么写的？
			}else{
				//从user_url到cur_url
				write2db(user_url,cur_url);
			}
		}
	}

	
}
