package javaapplication2;

import java.lang.*;
import java.sql.*;
import java.io.*;

class InputDataGenerator
{
//   public static void main(String args[]) throws Exception
    public static void generateInputData() throws Exception
    {
        //Database Connection
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://13.58.40.245:3306/noise", "noiseuser", "D)r27P(n31A+v31C-a15H&a31");
        PreparedStatement stm = con.prepareStatement("Select * from samples");
        ResultSet rs = stm.executeQuery();

//        try (PrintWriter pw = new PrintWriter(new FileWriter("d:/hadoop-3.2.1/bin/input/dataset.csv")))
        try (PrintWriter pw = new PrintWriter(new FileWriter("/mnt/d/hadoop-3.2.1/bin/input/dataset.csv")))
        {
            while (rs.next())
            {
                Object[] s =
                {
                    rs.getString(1), rs.getString(2), rs.getTimestamp(3).getTime(), rs.getString(4), rs.getString(5), rs.getString(6)
                };
                StringBuilder sb = new StringBuilder();
                for (Object ss : s)
                {
                    sb.append(ss).append(",");
                }
                if (sb.length() > 0)
                {
                    sb.deleteCharAt(sb.length() - 1);
                }
                pw.println(sb);
            }
        }

    }
}
