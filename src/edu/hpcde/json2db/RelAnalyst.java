package edu.hpcde.json2db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RelAnalyst extends JsonConverter{

	public RelAnalyst() throws Exception {
		super();
		// TODO Auto-generated constructor stub
	}
	public void run() throws SQLException{
		String sql = "select * from WEIBOREPOST where `mid`=? and rownum<11".replace("`", "\"");
		String mid = "ABFMmAs8r";
		con.setAutoCommit(false);
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1,mid);
		ResultSet rs  = ps.executeQuery();
		while(rs.next()){
			String repostid = rs.getString("repost_id");
			String repoststring = rs.getString("repoststring");
			String reg = "";
			Pattern pattern = Pattern.compile(reg);
			Matcher matcher = pattern.matcher(repoststring);
			
			int l = repoststring.indexOf("//@");
			String temstr = repoststring;
			
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
