package com.qts;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.zxing.EncodeHintType;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import com.model.Book;

public class BookEntry {
	
	private static final String FILE_NAME = "D:\\Work\\SSR\\Data Received From SSR During Visit\\Our edited excels for data loading\\Library Accession_Register_modified.xlsx";
	Connection con = null;
	Statement  stmt = null;
	List<Book> bookList = null;
	HashSet<String>publisher = new HashSet<>();
	HashSet<String>bookAuthor = new HashSet<>();
	HashSet<String>title = new HashSet<>();
	
	public static void main(String args[]){
		
		BookEntry entry = new BookEntry();
		
		entry.insertookInDB(entry.readExcel());
	}
	
	public void insertookInDB(List<Book> bookList){
		
	}
	
	public List<Book> readExcel(){
	
		bookList = new ArrayList<Book>();
		
		try{
			
			FileInputStream excelFile = new FileInputStream(new File(FILE_NAME));
            Workbook workbook = new XSSFWorkbook(excelFile);
            Sheet datatypeSheet = workbook.getSheetAt(0);
                       
            int startRow = datatypeSheet.getFirstRowNum() + 1; 
            int lastRow = datatypeSheet.getLastRowNum();
            
            for(int count = startRow; count <= lastRow; count ++ ) {

        	   Book book = new Book();
        	   Row currentRow = datatypeSheet.getRow(count);  
               
        	   book.setAccessionNo(new Double(currentRow.getCell(0).getNumericCellValue()).intValue());
        	   //System.out.println(book.getAccessionNo());
        	   book.setAccessionDate(currentRow.getCell(1).getDateCellValue().toString());
        	   //System.out.println(book.getAccessionDate());
        	   book.setAuthor(currentRow.getCell(2).getStringCellValue());
        	   if(null != currentRow.getCell(6).getStringCellValue() || currentRow.getCell(6).getStringCellValue() != ""){
        		   bookAuthor.add(currentRow.getCell(2).getStringCellValue());
        	   }
        	   
        	   //System.out.println(book.getAuthor());
        	   book.setTitle(currentRow.getCell(3).getStringCellValue());
        	   if(null != currentRow.getCell(3).getStringCellValue() || currentRow.getCell(3).getStringCellValue() != ""){
        		   title.add(currentRow.getCell(3).getStringCellValue());
        	   }
        	   //System.out.println(book.getTitle());
        	   book.setPubYear(String.valueOf(currentRow.getCell(4).getNumericCellValue()));
        	   //System.out.println(book.getPubYear());
        	   book.setPubPlace(currentRow.getCell(5).getStringCellValue());
        	   //System.out.println(book.getPubPlace());
        	   book.setPublisher(currentRow.getCell(6).getStringCellValue());
        	   if(null != currentRow.getCell(6).getStringCellValue() || currentRow.getCell(6).getStringCellValue() != ""){
        		   publisher.add(currentRow.getCell(6).getStringCellValue());
        	   }
        	  
        	   //System.out.println(book.getPublisher());
        	   //System.out.println(count);
        	   bookList.add(book) ;
        	   
            }
           // System.out.println("publisher : "+publisher);
            List<String>publisherList = new ArrayList<>();//for publisher
            publisherList.addAll(publisher);
            insertBookPublisher(publisherList);
            
            
            List<String>authorList = new ArrayList<>();
            authorList.addAll(bookAuthor);
            insertAuthor(authorList); //for  author
            
           
            insertIntoLibraryCatalogue(bookList); //for libraryCatalogue
            
            insertIntoBook(bookList); //for book
            
            insertIntoBookAuthor(bookList); //for book author
            
            /*for(Book b : bookList){
            	 System.out.println(b.getAccessionNo() + " : " + b.getAccessionDate() + " : " + b.getAuthor() + " : " + b.getTitle() + " : " + b.getPubYear() + " : " + b.getPubPlace() + " : " + b.getPublisher());
            }*/
		}catch(IOException ex){
			
			ex.printStackTrace();
		}
		
		return bookList;
	}
	
	public Connection jdbcConnection(){
		 try {
			Class.forName("org.postgresql.Driver");
			con = DriverManager
		            .getConnection("jdbc:postgresql://localhost:5432/iCAM_SSRewa",
		            "postgres", "postgres");
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        System.out.println("Opened database successfully");
        return con;
	}
	
	public void insertBookPublisher(List<String> publisherList){
		con = jdbcConnection();
		
		if(null !=con){
			try {
				
				stmt = con.createStatement();
				for(String publisher : publisherList){
					
					String sql = "INSERT INTO book_publisher(rec_id, obj_id, updated_by, updated_on, date_of_creation, book_publisher_code, book_publisher_name, book_publisher_desc)"
							+"VALUES (uuid_generate_v4(), 'BOOK_PUBLISHER_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()), (select 'PUB'|| '_' ||COALESCE((SELECT MAX(serial_id) FROM book_publisher), 0)+1), '"+publisher+"', '"+publisher+"');";
					stmt.addBatch(sql);
				}
				stmt.executeBatch();
				
			    System.out.println("book publisher inserted");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("failed to insert book publisher");
			}finally{
				try {
					stmt.close();
					
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		         
			}
		}
	}
	
	
	
	public void insertAuthor(List<String> authorList){
		con = jdbcConnection();
		
		if(null !=con){
			try {
				stmt = con.createStatement();
				for(String author : authorList){
					String sql = "INSERT INTO author(rec_id, obj_id, updated_by, updated_on, date_of_creation, author_full_name)"
							+"VALUES (uuid_generate_v4(), 'AUTHOR_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()),  '"+author+"');";
					
					//stmt.executeUpdate(sql);
					stmt.addBatch(sql);
				}
				stmt.executeBatch();
				System.out.println("author inserted successfully");
			     
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("failed to insert author");
			}finally{
				try {
					stmt.close();
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		         
			}
		}
	}
	
	public void insertIntoLibraryCatalogue(List<Book> bookList){
		con = jdbcConnection();
		String title = "";
		String publisher = "";
		String place = "";
		String year = "";
		
		if(null !=con){
			try {
				stmt = con.createStatement();
				title = bookList.get(0).getTitle();
				publisher = bookList.get(0).getPublisher();
				place = bookList.get(0).getPubPlace();
				year = bookList.get(0).getPubYear().replace(".0", "");
				
				String sql = "INSERT INTO library_catalogue(rec_id, obj_id, updated_by, updated_on, date_of_creation, item_code, item_name, book_publisher, place, publish_year, type, item_status)"
							+"VALUES (uuid_generate_v4(), 'LIBRARY_CATALOGUE_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()),(select 'LC'|| '_' ||COALESCE((SELECT MAX(serial_id) FROM library_catalogue), 0)+1),'"+title+"',(SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true),'"+place+"','"+year+"',"
							+ "(SELECT rec_id FROM book_category WHERE book_category_code = 'BOOK_CATEGORY_1' AND is_active = true), (SELECT rec_id FROM status_of_item WHERE status_of_item_code = 'STATUSOFITEM-1' AND is_active = true));";
				
				stmt.executeUpdate(sql);
				
				for(Book book : bookList){
					
					title = book.getTitle();
					publisher = book.getPublisher();
					place = book.getPubPlace();
					year = book.getPubYear().replace(".0", "");
					
					String sql1 = "SELECT item_name FROM library_catalogue WHERE item_name = '"+title+"' AND place = '"+place+"' AND publish_year = '"+year+"' AND book_publisher = (SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true);";
					ResultSet rs = stmt.executeQuery(sql1);
					//System.out.println("rs=="+rs);
					if(rs.next()){
						rs.close();
					}else {
						
						//itemName = rs.getString("item_name");
						//System.out.println(itemName);
						
						//if(itemName == ""){
							//System.out.println("within insert");
							String sql2 =  "INSERT INTO library_catalogue(rec_id, obj_id, updated_by, updated_on, date_of_creation, item_code, item_name, book_publisher, place, publish_year, type, item_status)"
									+"VALUES (uuid_generate_v4(), 'LIBRARY_CATALOGUE_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()),(select 'LC'|| '_' ||COALESCE((SELECT MAX(serial_id) FROM library_catalogue), 0)+1),'"+title+"',(SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true),'"+place+"','"+year+"',"
									+ "(SELECT rec_id FROM book_category WHERE book_category_code = 'BOOK_CATEGORY_1' AND is_active = true), (SELECT rec_id FROM status_of_item WHERE status_of_item_code = 'STATUSOFITEM-1' AND is_active = true));";
						
							stmt.executeUpdate(sql2);
						//}
						
						
					}
					//rs.close();
				}

				System.out.println("Library catalogue inserted successfully");
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("failed to insert library catalogue");
			}finally{
				try {
					stmt.close();
					con.close();
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		         
			}
			
		}
	}
	public void insertIntoBook(List<Book> bookList){
			con = jdbcConnection();
			String title = "";
			String publisher = "";
			String place = "";
			String year = "";
			
			String accNo = "";
			String accDate = "";
			
			if(null !=con){
				try {
					stmt = con.createStatement();
					for(Book book : bookList){
						
						title = book.getTitle();
						publisher = book.getPublisher();
						place = book.getPubPlace();
						year = book.getPubYear().replace(".0", "");
						accNo = book.getAccessionNo()+"";
						accDate = book.getAccessionDate();
						
						DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
						Date date = null;
						try {
							date = (Date)formatter.parse(accDate);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						      

						Calendar cal = Calendar.getInstance();
						cal.setTime(date);
						String formatedDate = cal.get(Calendar.DATE) + "/" + (cal.get(Calendar.MONTH) + 1) + "/" +         cal.get(Calendar.YEAR);
						//System.out.println("formatedDate : " + formatedDate); 
					     
						
						
						
						String sql = "INSERT INTO book (rec_id, obj_id, updated_by, updated_on, date_of_creation, book_code, date_of_entry, accession_number,library_catalogue, item_status, total_no_of_copies_available)"
									+"VALUES(uuid_generate_v4(), 'BOOK_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()),(select 'BOOK'|| '_' ||COALESCE((SELECT MAX(serial_id) FROM book), 0)+1), (SELECT extract(epoch from (SELECT to_timestamp('"+formatedDate+"','DD/MM/YYYY')))) ,'"+accNo+"',(SELECT rec_id FROM library_catalogue WHERE item_name = '"+title+"' AND place = '"+place+"' AND publish_year = '"+year+"' AND book_publisher = (SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true)),(SELECT rec_id FROM status_of_item WHERE status_of_item_code = 'STATUSOFITEM-2' AND is_active = true), 1);";
					stmt.executeUpdate(sql);
						
						String qrCodeData = accNo;
						String path = "D:\\Work\\SSR\\iCAM_SSRewa_Repository\\QRCode\\Book\\" + accNo+".png";	
						File dir = new File(path);
						boolean isDirCreated = dir.mkdirs();
						if (isDirCreated) {
							System.out.println("created  path "+path);
						}else
							System.out.println(path+ " already exist");
						generateQrCodeForBook(qrCodeData, path, dir);
					
					}
					System.out.println("inserted succesfully in book table");
				}catch(SQLException e){
					e.printStackTrace();
				}finally{
					try {
						stmt.close();
						con.close();
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			         
				}
			}
		
	}
	public void insertIntoBookAuthor(List<Book> bookList){
		con = jdbcConnection();
		String title = "";
		String publisher = "";
		String place = "";
		String year = "";
		String author = "";
		
		if(null !=con){
			try {
				stmt = con.createStatement();
				for(Book book : bookList){
					
					title = book.getTitle();
					publisher = book.getPublisher();
					place = book.getPubPlace();
					year = book.getPubYear().replace(".0", "");
					author = book.getAuthor();
					
				
					
					String sql1 = "SELECT rec_id FROM book_author WHERE author = (SELECT rec_id FROM author WHERE author_full_name = '"+author+"' AND is_active = true) AND catalogue_item = (SELECT rec_id FROM library_catalogue WHERE item_name = '"+title+"' AND place = '"+place+"' AND publish_year = '"+year+"' AND book_publisher = (SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true));";
					
					ResultSet rs = stmt.executeQuery(sql1);
					//System.out.println("rs=="+rs);
					if(rs.next()){
						rs.close();
					}else {
						
						
						String sql = "INSERT INTO book_author (rec_id, obj_id, updated_by, updated_on, date_of_creation, author, catalogue_item)"
								+"VALUES(uuid_generate_v4(), 'BOOK_AUTHOR_OLD_DATA_OBJ_ID', (SELECT rec_id FROM resource WHERE user_id ilike 'superadmin' AND is_active =true), extract(epoch FROM now()), extract(epoch FROM now()),(SELECT rec_id FROM author WHERE author_full_name = '"+author+"' AND is_active = true),(SELECT rec_id FROM library_catalogue WHERE item_name = '"+title+"' AND place = '"+place+"' AND publish_year = '"+year+"' AND book_publisher = (SELECT rec_id FROM book_publisher WHERE book_publisher_name = '"+publisher+"' AND is_active = true)) );";
				
						//System.out.println(sql);
						stmt.executeUpdate(sql);
					}
					
                               
				
				
				}
				System.out.println("inserted succesfully in book_author table");
			}catch(SQLException e){
				System.out.println("failed to insert");
				e.printStackTrace();
			}finally{
				try {
					stmt.close();
					con.close();
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		         
			}
		}
	}
	
	public void generateQrCodeForBook(String qrCodeData, String path, File dir){				
		try{
			String charset = "UTF-8"; // or "ISO-8859-1"
			Hashtable<EncodeHintType, ErrorCorrectionLevel> hintMap = new Hashtable<EncodeHintType, ErrorCorrectionLevel>();
			hintMap.put(EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.L);
			
			QRCodeUtility qrUtil = new QRCodeUtility();								
			qrUtil.createQRCode(qrCodeData, path, charset, dir, hintMap, 200, 200);			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
