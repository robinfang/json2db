package edu.hpcde.json2db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.codec.digest.DigestUtils;

public class RelAnalyst extends JsonConverter {
	private PrintWriter out;
	public RelAnalyst() throws Exception {
		super();
		this.out=new PrintWriter(new BufferedWriter(new FileWriter("f:\\myfile.csv", true)));
	}
	public void destory(){
		super.destory();
		out.close();
	}
	
	public int getRepostSouce(String substr) throws SQLException{
		substr = substr.split("赞")[0].replaceAll("\\s","");
		logger.info("3 substr: "+substr);
		String sql = "select `repost_id`,`repoststring` from weiborepost where `mid`=? and `repoststring` like ?".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1,"ABFMmAs8r");
		ps.setString(2, substr+"%");
		ResultSet rs = ps.executeQuery();
		int repostid=-1;
		if(rs.next()){
			repostid = rs.getInt(1);
			logger.info("4 source repost_id: "+ repostid);
			logger.info("5 source repoststring: " + rs.getString(2));
		}
		closeAll(rs,ps,null);
		return repostid;
		
	}
	public void writeOneLine(int n,int source_n) throws IOException{
		    this.out.println(n+","+source_n);
	}
	
	public void genRel() throws SQLException, Exception{
		//con.setAutoCommit(false);
		String sql = "select `repost_id`,`repoststring` from weiborepost where `mid`=? ".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, "ABFMmAs8r");
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			int repostid = rs.getInt(1);
			String repoststring = rs.getString(2);
			String[] sp = repoststring.split("//@");
			if(sp.length >1){
				logger.info("1 repost_id: "+repostid);
				logger.info("2 repost_string: "+repoststring);
				int fromid = getRepostSouce(sp[1]);
				if(fromid==-1){
					//repostid到origin
					writeOneLine(repostid, -1);
				}else{
					//repostid到fromid
					writeOneLine(repostid, fromid);
				}
			}else{
				//repostid到origin
				writeOneLine(repostid, -1);
			}
		}
		closeAll(rs,ps,null);
		out.close();
		//con.commit();
	}
	public static void main(String[] args) throws Exception {
		RelAnalyst ra = new RelAnalyst();
		ra.genRel();
		ra.destory();
	}
}
