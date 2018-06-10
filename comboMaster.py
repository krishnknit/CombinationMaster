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
#from sqlalchemy import create_engine


class ComboMaster(object):
	def __init__(self):
		## excel file path
		self.path = os.getcwd()
		self.fmlyDict = {}
		self.combDict = {}
		self.finalDict = {}
		self.scenario_fml_val = []
		self.numbyden = []

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



	def readData(self):
		''' read data from database to generate dict '''
		logging.info("getting database connection ...")
		con, cur = self.getConnection()

		try:
			logging.info("selecting records from database table ...")
			sql1 = "SELECT * from combotbl1"
			sql2 = "SELECT * from combotbl2"
			self.df1 = pd.read_sql(sql1, con)
			self.df2 = pd.read_sql(sql2, con)

			#print self.df1
			#print self.df2
			for index, row in self.df1.iterrows():
				self.fmlyDict.setdefault(row['scebdt'], []).append(row['fmly_id'])

		except Exception as e:
			logging.error("readData(), e: {}".format(e))
		else:
			## close database connection
			con.close()


	def combinations(self, tgt, data, key):
		for i in range(len(data)):
			new_tgt = copy.copy(tgt)
			new_data = copy.copy(data)
			new_tgt.append(data[i])
			new_data = data[i+1:]
			#print new_tgt
			self.combDict[tuple(new_tgt)] = key
			self.combinations(new_tgt, new_data, key)


	def createCombination(self):
		''' function to create combination all fmly_id uniquely '''
		try:
			self.fdf = pd.DataFrame(columns=['combfmlyid', 'scebdt', 'combtotal'])
			
			for key in self.fmlyDict.keys():
				self.combinations([], self.fmlyDict[key], key)
				
				for comb in self.combDict.keys():
					total = 0
					
					for c in comb:
						total += self.df1.loc[(self.df1['scebdt'] == key) & (self.df1['fmly_id'] == c), 'def'].iloc[0]

					self.finalDict[comb] = {}
					self.finalDict[comb][self.combDict[comb]] = total
					self.fdf = self.fdf.append({'combfmlyid': list(comb), 'scebdt': key, 'combtotal': total}, ignore_index=True)
					#print self.finalDict
				
				## generate numerator/denomenator
				self.generateNumbyDen()

			self.fdf['scenario_fml_val'] = self.scenario_fml_val
			self.fdf['numbyden'] = self.numbyden
			#print self.fdf
			print self.fdf.loc[self.fdf['numbyden'] != 'na', ['combfmlyid', 'scebdt', 'numbyden']]
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def generateNumbyDen(self):
		try:
			## get total of fmlycfr column
			total_cfr = self.df2['fmlycfr'].sum()

			for key in self.finalDict.keys():
				#print self.finalDict[key].keys()
				value = total_cfr
				for k in key:
					value -= self.df2.loc[self.df2['fmly_id'] == k, 'fmlycfr'].iloc[0]

				for scen in self.finalDict[key].keys():
					if value != 0:
						result = float(self.finalDict[key][scen])/value
						self.scenario_fml_val.append(result)

						if result > float(self.target):
							self.numbyden.append(result)
						else:
							self.numbyden.append('na')
					else:
						self.scenario_fml_val.append(0)
						self.numbyden.append('na')
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