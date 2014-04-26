package edu.hpcde.json2db;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeSeriesGetter extends RelAnalyst{

	public TimeSeriesGetter() throws Exception {
		super();
		// TODO Auto-generated constructor stub
	}
	public static String date2String(Date time){
		DateFormat df = new SimpleDateFormat("yyyyMMdd-HHmm");
		String reportDate = df.format(time);
		return reportDate;
	}
	public void writeOneLine(String str) throws IOException{
		PrintWriter out=new PrintWriter(new BufferedWriter(new FileWriter("f:\\time.txt", true)));
		logger.info(str);
		out.println(str);
		out.close();
	}
	public void getRepostTime(String mid) throws Exception{
		String sql = "select `time` from weiborepost where `mid`=? order by `time`".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1,mid);
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			writeOneLine(rs.getString(1));
		}
		closeAll(rs,ps,null);
	}
	public void getPostTime(String mid) throws Exception{
		String sql = "select `time` from weibopost where `mid`=?".replace("`", "\"");
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1,mid);
		ResultSet rs = ps.executeQuery();
		if(rs.next()){
			writeOneLine(rs.getString(1));
		}
		closeAll(rs,ps,null);
	}
	public static void main(String[] args) throws Exception{
		TimeSeriesGetter ts = new TimeSeriesGetter();
		ts.getPostTime("ABFMmAs8r");
		ts.getRepostTime("ABFMmAs8r");
		ts.destory();
	}

}
