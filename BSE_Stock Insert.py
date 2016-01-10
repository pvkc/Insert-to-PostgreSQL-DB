#!/usr/bin/python
# This program is written for and tested in Python 2.7.6
# The program reads a CSV file, formats the data and store it the database
# BSE releases a summarized details of each stock traded on daily basis, 
# 	the file will have following naming convention 
#	EQ<2-digit day><2-digit month><2-digit-year>
#   Ex: Stocks traded on 1-Jan-2015, will be in the file  EQ010115
# All the files of a particular month are stored in a folder, 
#	each file is read, formated and stored in the db
# The month is passed as argument to the program, 
#	so all the data for a month is read and stored
#
# Usage: python BSE_Stock Insert.py <month>
#
import psycopg2
import sys
import datetime
import calendar
import getpass

# Increase the debug level to get more debug data from the program
DEBUG_LEVEL = 0

# Location of the Errorlog file
LogFile="/home/pvkc/Desktop/StockMarket/BSE/Errorlog.txt"
con=None

# Function that opens the errorLog 
def OpenErrorLog():
	try:
		errorFile=open(LogFile,"a");
		return errorFile
	except Exception,exp:
		print exp		
		
# Function that write to the Errorfile
def WriteErrorLog (e):
	errorFile=OpenErrorLog()
	if errorFile == None:
		print "Problem With Opening ErrorLog File"
		return
	else:
		try:
			errorFile.write(str(datetime.datetime.now()) + " "+ "["+ getpass.getuser() + "]" + str(e)+"\n")
		except Exception,e:			
			print e			
		finally:
			errorFile.close()			

# Print the arguments passed, the month of trading to be stored
if DEBUG_LEVEL == 9:
	print sys.argv

# If no arguments are passed, the current month is used as default
if sys.argv[1] == None:
	month=datetime.datetime.now().month
else:
	# If argument is not an integer exit the program
	try:		
		month=int(sys.argv[1])
	except Exception,exp:
		print exp
		WriteErrorLog(exp)
		sys.exit(1)

monthWords=calendar.month_name[month]

# Connect to database, if unable to connect, write to log and exit
try:
	con=psycopg2.connect(database="stock_market",user="stockbroker",password="stockbroker",host="localhost")
	cur=con.cursor()
except psycopg2.DatabaseError,e:
	print 'Error %s' %e
	WriteErrorLog(e)
	sys.exit(1)

# Read all the files in the folder
for i in range(1,32):
	date=str(i).zfill(2)+str(month).zfill(2)+"15"
	filePath="/home/pvkc/Desktop/StockMarket/BSE/"+monthWords[0:3]+"/EQ" + date +".CSV"
	print filePath
	# Read the input file, if unable to open write to error log and skip to next
	try:
		inputFile = open(filePath,'r')
	except Exception,e: 
		print e
		WriteErrorLog(e)
		continue
		
	# SQL to execute
	SQL = "INSERT INTO BSE_STOCK VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
	
	# Determine the date of trading
	SQL_DATE="20"+date[4:6]+'-'+date[2:4]+'-'+date[0:2]
	
	# For each row in the file, split in to columns and store it in the DB
	for linenumber,row in enumerate(inputFile):		
		# Removes '\n\r' at the end of each row
		row=row.rstrip();
		# Split based on ','
		column=row.split(',')
		# Remove spaces at the end of stock name 
		column[1]=column[1].rstrip()
		# Insert trade date in between columns
		column.insert(1,SQL_DATE)
		# Print the row to be inserted for every 100 lines
		if linenumber%100==0 and DEBUG_LEVEL>5:
			print column 
		# The first line of file is Header, Skip it
		# Store the rest of the rows of filews
		# If Insert of a row fails write to error log and Skip to next file
		if linenumber>0:
			try:
				cur.execute(SQL,column)
			except Exception,exp:
				WriteErrorLog(exp)
				print exp
				break

	# Commit for each file stored
	con.commit()
# Close the connection
con.close()



