import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DatabaseMetaData;
import java.util.Properties;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.lang.Math;

public class pg{
	private static String write_schema = "huanran";
	private static String read_schema = "hw2";

	public static void main(String[] args) throws Exception{
		String url = "jdbc:postgresql://stampy.cs.wisc.edu/cs564instr?sslfactory=org.postgresql.ssl.NonValidatingFactory&ssl";
		Connection conn = DriverManager.getConnection(url); 
		Statement st = conn.createStatement();
		DatabaseMetaData dbm = conn.getMetaData();

		st.executeUpdate("SET search_path TO hw2;");

		Scanner scn = new Scanner(System.in);
		int ifquit = 0;
		do{	
			try{
			//ask for name
			System.out.println("Tell me your table name or query.");
			String name = scn.next();
			ResultSet rs_name = dbm.getTables(null, read_schema, name.toLowerCase(), null);
			
			//test if table exists
			if(rs_name.next()){

				//ask for amount
				System.out.print("How many samples do you want from the table: ");
				int n = scn.nextInt();

				//ask for table created
				System.out.print("Do you want a table created for the sampled table? (1 for yes/ 0 for no) ");
				int table_create = scn.nextInt();
				if(table_create == 0){
					output_sample(st, n, name);
				}
				else{
					System.out.print("Type the name of the table: ");
					String table_name = scn.next();
					
					//detect if table_name is already existing
					ResultSet tb_name = dbm.getTables(null, write_schema, table_name.toLowerCase(), null);
					while(tb_name.next()){
						System.out.println("Table name already exists. Please try another one: ");
						table_name = scn.next();
						tb_name = dbm.getTables(null, write_schema, name.toLowerCase(), null);
					}
					System.out.print("What schema do you want to save the table: ");
					write_schema = scn.next();

					table_sample(st, n, name, table_name);
				}
			}
			else {
				//read the sql and execute.
				String sql_cmd = name + " ";
				/**
				try{
					File file = new File(name);
					Scanner file_input = new Scanner(file);
					while(file_input.hasNextLine()){
						String line = file_input.nextLine();
						sql_cmd += line + "\n";
					}
				}catch( FileNotFoundException e){
					System.out.println("Not Found!");
				}
				**/
				while(true){
					String word = scn.next();
					if(word.contains(";")){
						word = word.substring(0, word.indexOf(";") + 1);
						sql_cmd += word;
						break;
					}
					else sql_cmd += word + " ";
				}
				//System.out.println(sql_cmd);
				try{
					boolean state = st.execute(sql_cmd);
					if(state == true){
						ResultSet rs = st.getResultSet();

						String sql = "";
						for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){ 
							System.out.print(rs.getMetaData().getColumnName(i) + "\t|\t");
						}
						System.out.println();
						while(rs.next()){
				  			for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){
								System.out.print(rs.getObject(i) + "\t|\t");
							}
							System.out.println();
						}
					}
				//catch the exception when sql_cmd is invalid
				}catch (Exception e){
					System.out.println(e.getMessage());
				}
			}

			rs_name.close();

			//catch the exception if any input is not in the correct format
			}catch(Exception e){
				System.out.println(e.getMessage());
			}


			System.out.print("Do you want to sample more? (1 for yes, 0 for quit)? ");
			ifquit = scn.nextInt();
		}while ( ifquit == 1);

		st.close();
		conn.close();
	}

	private static void table_sample(Statement st, int amount, String name, String table_name) throws Exception{

		//store all random generized index
		ArrayList<Integer> indeces = new ArrayList<Integer>();
		
		//find the length
		ResultSet rs = st.executeQuery("select count(*) from " + name);
		rs.next();
		int a = rs.getInt(1);
		
		generate_sequence(amount, indeces, a);

		//set up the table
		System.out.println("###############NOTE: Table will be created in the schema named: " + write_schema);
		String sql = "CREATE TABLE " + write_schema + "." + table_name + " (";
		rs = st.executeQuery("select * from " + name); 
		
		for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){ //what about primary key?
			sql += rs.getMetaData().getColumnName(i) + " " + rs.getMetaData().getColumnTypeName(i);
			if (i != rs.getMetaData().getColumnCount()) sql += ", ";
			else sql += ");\n";
		}

		System.out.println(sql);
		st.executeUpdate(sql);

		sql = "";


		rs = st.executeQuery("select * from " + name); 
		//insert the element
		int index = 0;
		while(rs.next()) {
			if(indeces.contains(index)){
				sql += "INSERT INTO " + write_schema + "." + "\"" + table_name + "\" VALUES(";
			  	for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){
					sql += "\'" + rs.getObject(i)  + "\'";
					if (i != rs.getMetaData().getColumnCount()) sql += ", ";
					else sql += ");\n";
				}	
			}
			index ++;
		}

		System.out.println(sql);
		st.executeUpdate(sql);
		rs.close();


	}
	
	private static void output_sample(Statement st, int amount, String name) throws Exception{
		//store all random generized index
		ArrayList<Integer> indeces = new ArrayList<Integer>();
		
		//find the length
		ResultSet rs = st.executeQuery("select count(*) from " + name); 
		rs.next();
		int a = rs.getInt(1);
		
		generate_sequence(amount, indeces, a);
		
		//form the list
		rs = st.executeQuery("select * from " + name);
		int index = 0;
		
		//print the list title
		for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){
			System.out.print(rs.getMetaData().getColumnName(i) + "\t|\t");
		}
		
		//print the list
		System.out.println();
		while(rs.next()) {
			if(indeces.contains(index)){
			  	for(int i = 1; i < rs.getMetaData().getColumnCount() + 1; ++i){
					System.out.print(rs.getObject(i) + "\t|\t");
				}	
				System.out.println();
			}
			index ++;
		}

		rs.close();
	}

	private static void generate_sequence(int amount, ArrayList<Integer> indeces, int max){
		if (amount > max) {
			System.out.println("NOTE: The number of samples exceeds the maximum rows of the table. All rows are inserted into the result.");
			for(int i = 0; i < max; ++i)
				indeces.add(i);
		}
		else{
			int t = 0;
			int m = 0;
			Random rand;

			//ask for random seed
			Scanner input = new Scanner(System.in);
			System.out.println("Do you want to set the seed? (1 for yes/ 0 for no): ");
			int set = input.nextInt();
			if( set == 1){
				System.out.println("Type the Seed: ");
				int seed = input.nextInt();
				rand = new Random(seed);
			}
			else{
				rand = new Random();
			}

			// Algo from Knuth
			while(true){

				Double U = rand.nextDouble();;
				if((max - t) * U >= (amount - m)){
					t++;
					continue;
				}
				else{
					Double dou = U * max;
					int temp = dou.intValue();
					if(indeces.contains(temp)) continue;
					indeces.add(temp);
					m++;
					t++;
					if ( m >= amount) break;
				}

			}	


		}
	}

}
