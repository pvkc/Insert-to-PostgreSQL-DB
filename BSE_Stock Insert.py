#!/usr/bin/python

import psycopg2
import sys
import datetime
import calendar
import getpass

DEBUG_LEVEL = 0
LogFile="/home/pvkc/Desktop/StockMarket/BSE/Errorlog.txt"
con=None

def OpenErrorLog():
	try:
		errorFile=open(LogFile,"a");
		return errorFile
	except Exception,exp:
		print exp		

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

if DEBUG_LEVEL == 9:
	print sys.argv

if sys.argv[1] == None:
	month=datetime.datetime.now().month
else:
	try:		
		month=int(sys.argv[1])
	except Exception,exp:
		print exp
		WriteErrorLog(exp)
		sys.exit(1)

monthWords=calendar.month_name[month]


try:
	con=psycopg2.connect(database="stock_market",user="stockbroker",password="stockbroker",host="localhost")
	cur=con.cursor()
except psycopg2.DatabaseError,e:
	print 'Error %s' %e
	WriteErrorLog(e)
	sys.exit(1)

for i in range(1,32):
	date=str(i).zfill(2)+str(month).zfill(2)+"15"
	filePath="/home/pvkc/Desktop/StockMarket/BSE/"+monthWords[0:3]+"/EQ" + date +".CSV"
	print filePath

	try:
		inputFile = open(filePath,'r')
	except Exception,e: 
		print e
		WriteErrorLog(e)
		continue
		
	SQL = "INSERT INTO BSE_STOCK VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
	SQL_DATE="20"+date[4:6]+'-'+date[2:4]+'-'+date[0:2]
	for linenumber,row in enumerate(inputFile):
		#row=inputFile.readline()
		row=row.rstrip();
		column=row.split(',')
		column[1]=column[1].rstrip()
		column.insert(1,SQL_DATE)
		if linenumber%100==0 and DEBUG_LEVEL>5:
			print column 
		if linenumber>0:
			try:
				cur.execute(SQL,column)
			except Exception,exp:
				WriteErrorLog(exp)
				print exp
				break

	con.commit()

con.close()



