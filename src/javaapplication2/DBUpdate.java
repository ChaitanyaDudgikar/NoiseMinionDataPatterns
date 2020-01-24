/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication2;

import java.sql.*;
import java.io.*;
import java.text.DateFormat;
//import java.text.DateFormat;  
import java.text.SimpleDateFormat;
import java.sql.Date;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import sun.misc.FloatingDecimal;
//import java.util.Date;

/**
 *
 * @author cdudg
 */
public class DBUpdate
{

    static
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex)
        {
            Logger.getLogger(DBUpdate.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static void main(String args[]) throws Exception
    {
        File file = new File("D:\\hadoop-3.2.1\\bin\\output\\part-r-00000"); //creates a new file instance  
        // Connection con = DriverManager.getConnection("jdbc:mysql://13.58.40.245:3306/noise", "noiseuser", "D)r27P(n31A+v31C-a15H&a31");
        try (Connection con = DriverManager.getConnection("jdbc:mysql://13.58.40.245:3306/noise", "noiseuser", "D)r27P(n31A+v31C-a15H&a31");
                PreparedStatement stm = con.prepareStatement("insert into noisebydate values(0,?,?,?,?,?) on duplicate key update noiselevel=?");
                PreparedStatement stm2 = con.prepareStatement("insert into noisepattern values(0,?,?,?,?,?,?,?) on duplicate key update noiselevel=?");
                FileReader fr = new FileReader(file); //reads the file  
                BufferedReader br = new BufferedReader(fr); //creates a buffering character input stream  
                )
        {
            StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters  
            String line;

            Calendar c = Calendar.getInstance();
            //Time t = new Time(0);
            //stm.setTime(2, t);

            while ((line = br.readLine()) != null)
            {
                String[] data = line.split("\t", 2);

                String str1 = data[0];
                String str3 = data[1];
                //System.out.println(data[1]);

                String[] s2 = str3.split(" ", 2);
                String str2 = s2[0];

                String[] s = str1.split("_", 7);
                if (s[0].equals("DATE"))
                {
                    
                    int date = Integer.parseInt(s[1]);
                    int month = Integer.parseInt(s[2]);
                    int year = Integer.parseInt(s[3]);
                    int time = Integer.parseInt(s[4]);
                    double longitude = Double.parseDouble(s[5]);
                    double latitude = Double.parseDouble(s[6]);
                    double noiselevel = Double.parseDouble(str2);

                   
                    c.set(Calendar.YEAR, year);
                    c.set(Calendar.MONTH, month);
                    c.set(Calendar.DAY_OF_MONTH, date);
                    c.set(Calendar.HOUR_OF_DAY, time);
                    c.set(Calendar.MINUTE, 0);
                    c.set(Calendar.SECOND, 0);

                    Date d = new Date(c.getTimeInMillis());
                    Time t = new Time(c.getTimeInMillis());
                    //System.out.println(d);
                    //String strDate = formatter.format(d);

                    stm.setTime(2, t);
                    //System.out.println("time" + t);

                    stm.setDate(1, d);
                    stm.setDouble(3, longitude);
                    stm.setDouble(4, latitude);
                    stm.setDouble(5, noiselevel);
                    stm.setDouble(6, noiselevel);

                    stm.executeUpdate();

                    
                }
                if (s[0].equals("PATTERN-01"))
                {
                   

                    double longitude = Double.parseDouble(s[1]);
                    double latitude = Double.parseDouble(s[2]);
                    double noiselevel = Double.parseDouble(str2);
                    
                    stm2.setString(1, "-");
                    stm2.setString(2, "-");
                    stm2.setString(3, "-");
                    stm2.setString(4, "-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }

                if (s[0].equals("PATTERN-02"))
                {
                    
                    String t=s[1];
                    double longitude = Double.parseDouble(s[2]);
                    double latitude = Double.parseDouble(s[3]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, "-");
                    stm2.setString(3, "-");
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                
                if (s[0].equals("PATTERN-03"))
                {
                    
                    String date=s[1];
                    double longitude = Double.parseDouble(s[2]);
                    double latitude = Double.parseDouble(s[3]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, date);
                    stm2.setString(2, "-");
                    stm2.setString(3, "-");
                    stm2.setString(4,"-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                
                if (s[0].equals("PATTERN-04"))
                {
                    
                    String dow=s[1];
                    double longitude = Double.parseDouble(s[2]);
                    double latitude = Double.parseDouble(s[3]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, "-");
                    stm2.setString(3, dow);
                    stm2.setString(4,"-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                
                if (s[0].equals("PATTERN-05"))
                {
                    
                    String date=s[1];
                    String t=s[2];
                    double longitude = Double.parseDouble(s[3]);
                    double latitude = Double.parseDouble(s[4]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, date);
                    stm2.setString(2, "-");
                    stm2.setString(3, "-");
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                
                 if (s[0].equals("PATTERN-06"))
                {
                    
                    String dow=s[1];
                    String t=s[2];
                    double longitude = Double.parseDouble(s[3]);
                    double latitude = Double.parseDouble(s[4]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, "-");
                    stm2.setString(3, dow);
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                 
                 if (s[0].equals("PATTERN-07"))
                {
                    
                    String month=s[1];
                    double longitude = Double.parseDouble(s[2]);
                    double latitude = Double.parseDouble(s[3]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, month);
                    stm2.setString(3, "-");
                    stm2.setString(4, "-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                 
                  if (s[0].equals("PATTERN-08"))
                {
                    
                    String month=s[1];
                    String t=s[2];
                    double longitude = Double.parseDouble(s[3]);
                    double latitude = Double.parseDouble(s[4]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, month);
                    stm2.setString(3, "-");
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }

                   if (s[0].equals("PATTERN-09"))
                {
                    
                    String month=s[1];
                    String date=s[2];
                    double longitude = Double.parseDouble(s[3]);
                    double latitude = Double.parseDouble(s[4]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, date);
                    stm2.setString(2, month);
                    stm2.setString(3, "-");
                    stm2.setString(4,"-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                   
                    if (s[0].equals("PATTERN-10"))
                {
                    
                    String month=s[1];
                    String dow=s[2];
                    double longitude = Double.parseDouble(s[3]);
                    double latitude = Double.parseDouble(s[4]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, month);
                    stm2.setString(3, dow);
                    stm2.setString(4,"-");
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }

                     if (s[0].equals("PATTERN-11"))
                {
                    
                    String month=s[1];
                    String date=s[2];
                    String t=s[3];
                    double longitude = Double.parseDouble(s[4]);
                    double latitude = Double.parseDouble(s[5]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, date);
                    stm2.setString(2, month);
                    stm2.setString(3, "-");
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }
                     
                   if (s[0].equals("PATTERN-12"))
                {
                    
                    String month=s[1];
                    String dow=s[2];
                    String t=s[3];
                    double longitude = Double.parseDouble(s[4]);
                    double latitude = Double.parseDouble(s[5]);
                    double noiselevel = Double.parseDouble(str2);

                    stm2.setString(1, "-");
                    stm2.setString(2, month);
                    stm2.setString(3, dow);
                    stm2.setString(4,t);
                    stm2.setDouble(5, longitude);
                    stm2.setDouble(6, latitude);
                    stm2.setDouble(7, noiselevel);
                    stm2.setDouble(8, noiselevel);
                    stm2.addBatch();
                }

                
            }
            System.out.println("Executing batch");
            stm2.executeBatch();
            fr.close();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
