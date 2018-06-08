#!/usr/bin/python
import os
import sys
import time
import copy
import pymssql
import logging
import datetime
import argparse
import subprocess
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


class ComboMaster(object):
	def __init__(self):
		## excel file path
		self.path = os.getcwd()
		self.fmlyDict = {}
		
		## credentials
		self.hostname = 'localhost'
		self.username = 'SA'
		self.password = 'cybage@123'
		self.dbname   = 'TestDB'
		self.port     = '1433'


	def getargs(self):
		if len (sys.argv) != 2 :
			print ("Usage: python <SCRIPT_NAME> <TARGET> ")
			sys.exit (1)
		else:
			self.target = sys.argv[1]


	def combinations(self, tgt, data):
		for i in range(len(data)):
			new_tgt = copy.copy(tgt)
			new_data = copy.copy(data)
			new_tgt.append(data[i])
			new_data = data[i+1:]
			print new_tgt
			self.combinations(new_tgt, new_data)


	def readData(self):
		''' read data from database to generate dict '''

		logging.info("getting database connection ...")
		con, cur = self.getConnection()

		try:
			logging.info("selecting records from database table ...")
			sql = "SELECT * from combotbl1"
			cur.execute(sql)
			rows = cur.fetchall()
			
			for row in rows:
				#print "row"
				bus_dt = row[0]
				fmly_id = row[1]
				snebdt = row[2]
				defi = row[3]

				## creating dictionary of scenariodate & famlyid
				self.fmlyDict.setdefault(snebdt, []).append(fmly_id)

			# print "self.fmlyDict"
			# print "self.fmlyDict.keys()""
		except Exception as e:
			logging.error("readData(), e: {}".format(e))
		else:
			## close database connection
			con.close()


	def createCombination(self):
		''' function to create combination all fmly_id uniquely '''
		try:
			for key in self.fmlyDict.keys():
				self.combinations([], self.fmlyDict[key])
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def getConnection(self):
		con = pymssql.connect(
					self.hostname,
					self.username,
					self.password,
					self.dbname,
					self.port)

		cursor = con.cursor()

		return con, cursor


	def main(self):
		logging.basicConfig(filename=os.path.join(os.getcwd(), 'comboMaster.log'),
							format='%(asctime)s: %(levelname)s: %(message)s',
							level=logging.DEBUG)

		logging.info("********************************************************")
		logging.info("starting combo master process ...")
		logging.info("********************************************************")
		stime = time.time()
		self.getargs()
		self.readData()
		self.createCombination()

		etime = time.time()
		ttime = etime - stime		
		logging.info("********************************************************")
		logging.info("time consumed to complete the process: {}".format(ttime))
		logging.info("completing combo master process ...")
		logging.info("********************************************************")


if __name__ == '__main__':
	com = ComboMaster()
	com.main()