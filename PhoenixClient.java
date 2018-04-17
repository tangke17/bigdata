package com.loco;
import java.sql.*;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
public class PhoenixClient {
    private Connection conn=null;
    private Statement stmt=null;
    static {
        try{
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }
    private Connection getConnection(String host,String port){
        Connection cc=null;
        final String url="jdbc:phoenix:"+host+":"+port;
        if(cc==null){
            try{
                final ExecutorService exec=Executors.newFixedThreadPool(1);
                Callable<Connection> call=new Callable<Connection>() {
                    @Override
                    public Connection call() throws Exception {
                        return DriverManager.getConnection(url);
                    }

                };
                Future<Connection> future=exec.submit(call);
                cc=future.get(1000 * 10, TimeUnit.MILLISECONDS);
                // cc=future.get();
                exec.shutdown();
            }catch (InterruptedException e){
                e.printStackTrace();
            }catch (ExecutionException e){
                e.printStackTrace();

            }
            catch (TimeoutException e){
                e.printStackTrace();
            }
        }
        return cc;
    }
    public PhoenixClient(String host,String port){
        if(host==null || port==null || host.trim()=="" || port.trim()==""){
            System.out.println( "Hbase Mast ip and port is needed");
            return;
        }
        try {
            conn = getConnection(host, port);
            if (conn == null) {
                System.out.println("Phoenix DB connection timeout");
                return;
            }
            stmt = conn.createStatement();
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
    public JSONArray execSql(String phoenixSQL){
        if (phoenixSQL==null || phoenixSQL.trim()==""){
            System.out.println( "PhoenixSQL is needed");
        }

        JSONArray result=new JSONArray();
        try{
            long startTime=System.currentTimeMillis();
            PhoenixResultSet set=(PhoenixResultSet) stmt.executeQuery(phoenixSQL);
            ResultSetMetaData meta=set.getMetaData();
            ArrayList<String> cols=new ArrayList<String>();
            JSONArray jsonArr=new JSONArray();
            while (set.next()){
                if(cols.size()==0){
                    for(int i=1,count=meta.getColumnCount();i<=count;i++){
                        cols.add(meta.getColumnName(i));
                    }
                }

                JSONObject json=new JSONObject();
                for(int i=0,len=cols.size();i<len;i++){

                    json.put(cols.get(i),set.getString(cols.get(i)));
                }
                jsonArr.put(json);
            }


            result=jsonArr;
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println( "SQL exec error:"+e.getMessage());
        }catch (JSONException e){
            e.printStackTrace();
            System.out.println("JSON error:"+e.getMessage());
        }
        return result;
    }
    public  void update(String phoenixSQL){
        if (phoenixSQL==null || phoenixSQL.trim()=="") {
            System.out.println("PhoenixSQL is needed");
            return;
        }
        try {
            stmt.executeUpdate(phoenixSQL);
            conn.commit();
        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

    }
    public void createTable(String phoenixSQL){
        System.out.println("phoenixSQL:"+phoenixSQL);
        if (phoenixSQL==null || phoenixSQL.trim()=="") {
            System.out.println("PhoenixSQL is needed");
            return;
        }
        String tablename=phoenixSQL.split(" ")[2].trim();

        try {

            PhoenixResultSet t_cnt = ( PhoenixResultSet)stmt.executeQuery("select count(DISTINCT TABLE_NAME)  from SYSTEM.CATALOG where table_name='" + tablename+"'");
            while (t_cnt.next()){
                if(t_cnt.getString("DISTINCT_COUNT(TABLE_NAME)").equals("0")) {
                    System.out.println("executing create table,t_cnt:" + t_cnt.getString("DISTINCT_COUNT(TABLE_NAME)"));
                    stmt.executeUpdate(phoenixSQL);
                }
                else {
                    System.out.println(tablename+" is exists,t_cnt:"+t_cnt.getString("DISTINCT_COUNT(TABLE_NAME)"));
                }
            }

        }
        catch (SQLException e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

}
