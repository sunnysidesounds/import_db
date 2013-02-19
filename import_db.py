#! /usr/bin/env python

################################################################################
## NAME: DATABASE IMPORT SCRIPT 
## DATE: Feb 2012
## AUTHOR: Jason R Alexander
## MAIL: JasonAlexander@zumiez.com
## SITE: http://www.zumiez.com
## INFO: This script import grandriver test database
#################################################################################

import sys
import os
import oursql
import string
import time
import base64
import csv
import datetime
from urllib import urlopen
import logging
import glob
import traceback
import ast
import urllib

#email
import smtplib
from email import Encoders
from email.MIMEBase import MIMEBase
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import formatdate
#zip
import zipfile
#ftp
import ftplib
#copy files
import shutil


class databaseImport():

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def __init__(self):
	    #DB
		self.database = '<database_name>'
		self.prodDb = self.database
		self.dbHost = '<host>' 
		self.sqlPath = '<path_to_sql_file>'
		self.sqlProdMatchNm = '<sql_file_name' #without .sql
		self.dbUsername = '<db_username>'
		self.dbPassword = '<db_password>'

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def dbConnect(self):
		"""Basic sql connect which creates a cursor to execute queries for prod """
		conn = oursql.connect(host = self.dbHost1, user=self.dbUsername, passwd=self.dbPassword,db=self.database, use_unicode=False, charset=None, port=3306)	
		curs = conn.cursor(oursql.DictCursor)
		curs = conn.cursor(try_plain_query=False)
		return curs
	
	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def getFilesFromPath(self):
		"""Get all files from a path and puts them into a list. """		
		directoryList = os.listdir(self.sqlPath)
		return directoryList

	# --------------------------------------------------------------------------------------------------------------------------------------------------------------- #
	def sendEmail(self, to_email, from_email, subject, text, file_attachment):
	    """This sends email """
	    
	    HOST = "localhost"    
	    TO = to_email
	    FROM = from_email
	    ATTACH = file_attachment
	 
	    msg = MIMEMultipart()
	    msg["From"] = FROM
	    msg["To"] = TO
	    msg["Subject"] = subject
	    msg['Date']    = formatdate(localtime=True)
	    
	    msg.attach( MIMEText(text) )	 
	    # attach a file
	    part = MIMEBase('application', "octet-stream")
	    part.set_payload( open(ATTACH,"rb").read() )
	    Encoders.encode_base64(part)
	    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(ATTACH))
	    msg.attach(part)	 
	    server = smtplib.SMTP(HOST)
	 
	    try:
	        failed = server.sendmail(FROM, TO, msg.as_string())
	        server.close()
	    except Exception, e:
	        errorMsg = "Unable to send email. Error: %s" % str(e)



if __name__ == '__main__':
	try:
		db = databaseImport()
		currentDate = datetime.datetime.now()	
		if(len(str(currentDate.month)) == 1):
			month = '0' + str(currentDate.month)
		else:
			month = str(currentDate.month)
		
		if(len(str(currentDate.day)) == 1):
			day = '0' + str(currentDate.day)
		else:
			day = str(currentDate.day)

		today = str(month) + "-" + str(day) + "-" + str(currentDate.year - 2000)
				
		print ""
		print 'Running database imports'
		print '------------------------------------------------------------------'

		sqlFiles =  db.getFilesFromPath()
				
		#Current  sql files
		prodFile = db.sqlProdMatchNm + today + '.sql'
				
		print "New files to be imported:"
		#Loading today's sql file into the database
		if(prodFile in sqlFiles):
			print " [" + prodFile + "] for " + db.prodDb
			#Remove this file from list
			sqlFiles.remove(prodFile)
			prodImportFile = db.sqlPath + '/' + prodFile
			#Setup mysql import for prod
			runCommand = "mysql -u "+db.dbUsername+" -p"+db.dbPassword+" -h "+db.dbHost1+" "+db.prodDb+" < "+prodImportFile+" "
			os.system(runCommand)
			
		print ""		
		print "Old files to be removed:"
		#Removing any sql files that aren't related to today's import.				
		count = 0
		for sqlFile in sqlFiles:			
			#Just delete .sql files
			if sqlFile.endswith('.sql'):
				print " [" + sqlFile + "] old sql file."
				sqlRemoveFile = db.sqlPath + '/' + sqlFile				
				#Remove old files
				os.remove(sqlRemoveFile)
			else:
				if(count == 1):
					print ' No old files to remove'			
			count = count +1

						
		print ""
		print "Success in import to " + db.prodDb

		print ""
		print '------------------------------------------------------------------'		
		
	except:
		lineNumber = traceback.extract_stack()[-1]
		errorLine = str(lineNumber[1])
		print ''
		print 'Fatal Script Error at Line '+errorLine
		print '-------------------------------------------------'
		traceback.print_exc(file=sys.stdout)
		print '-------------------------------------------------'
        sys.exit(1)
