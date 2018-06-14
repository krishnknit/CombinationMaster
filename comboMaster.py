#!/usr/bin/python
import os
import sys
import time
import copy
import config
import pymssql
import logging
import argparse
import datetime
import threading
#import numpy as np
import pandas as pd
from itertools import combinations


class ComboMaster(object):

	def __init__(self):
		''' Initialise the class with required valiables '''
		self.finalCombMinDF = pd.DataFrame(columns=['combfmlyid', 'scebdt', 'numbyden'])
		self.csvPath = os.getcwd()
		self.fmlyDict = {}
		self.combDict = {}
		self.finalDict = {}
		self.scenario_fml_val = []
		self.numbyden = []
		self.comblen = []


	def getargs(self):
		''' Read passed command line arguments '''

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
		''' To get all the unique combination of familyid for same scenario date '''

		for i in range(1,8):
			comb = combinations(data, i)
			for c in list(comb):
				self.combDict[tuple(c)] = key


	def createCombination(self):
		''' function to create combination all fmly_id uniquely '''

		try:
			self.fdf = pd.DataFrame(columns=['combfmlyid', 'scebdt', 'combtotal'])
			
			for key in self.fmlyDict.keys():
				self.combinations([], self.fmlyDict[key], key)
				
				for comb in self.combDict.keys():
					combtotal = 0
					
					for c in comb:
						compVal = self.df1.loc[(self.df1['scebdt'] == key) & (self.df1['fmly_id'] == c), 'def']
						if compVal.empty:
							continue
						combtotal += compVal.iloc[0]
					
					self.finalDict[comb] = {}
					self.finalDict[comb][self.combDict[comb]] = combtotal
					self.fdf = self.fdf.append({'combfmlyid': list(comb), 'scebdt': key, 'combtotal': combtotal}, ignore_index=True)
					#print self.finalDict
				
				## generate numerator/denomenator
				self.generateNumbyDen()

			self.fdf['scenario_fml_val'] = self.scenario_fml_val
			self.fdf['numbyden'] = self.numbyden
			self.fdf['comblen'] = self.comblen

			#print self.fdf
			#finalDf =  self.fdf.loc[(self.fdf['numbyden'] != 'na') & (self.fdf['combfilter'] != 'na'), ['combfmlyid', 'scebdt', 'numbyden']]
			finalDf =  self.fdf.loc[(self.fdf['numbyden'] != 'na'), ['combfmlyid', 'scebdt', 'numbyden', 'comblen']]
			#print finalDf
			
			self.generateMinComb(finalDf)
			self.generateCSVFile()
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def generateMinComb(self, finalDf):
		try:
			grouped = finalDf.groupby('scebdt')
			for name, group in grouped:
				combMinDF = pd.DataFrame(columns=['combfmlyid', 'scebdt', 'numbyden', 'comblen'])
				for index, row in finalDf.iterrows():
					if row['scebdt'] == name:
						combMinDF = combMinDF.append({'combfmlyid': row['combfmlyid'], 'scebdt': row['scebdt'], 'numbyden': row['numbyden'], 'comblen': row['comblen']}, ignore_index=True)
				
				gd = combMinDF.groupby('comblen')
				cnt = 0
				for n, g in gd:
				# 	print n
					minn = g.min()['numbyden']
					for i, r in finalDf.iterrows():
						if r['numbyden'] == minn:
							self.finalCombMinDF = self.finalCombMinDF.append({'combfmlyid': r['combfmlyid'], 'scebdt': r['scebdt'], 'numbyden': r['numbyden']}, ignore_index=True)
					cnt += 1
					if cnt == 1:
						break
		except Exception as e:
			logging.error("generateMinComb(), e: {}".format(e))


	def generateCSVFile(self):
		''' generate final dataframe to csv file '''

		today = datetime.date.today()
		today = today.strftime("%Y-%m-%d")
		#print "TODAY: ", today
		#print self.finalCombMinDF

		csvFile = 'combomaster_'+today+'.csv'
		try:
			file = os.path.join(self.csvPath, csvFile)
			logging.info("creating csv file '{}' ...".format(file))
			excelDF = self.finalCombMinDF.to_csv(file, index=False)
		except Exception as e:
			logging.error("generateCSVFile(), e: {}".format(e))


	def generateNumbyDen(self):
		''' To generate nunerator and denomenator combination '''
		
		try:
			## get total of fmlycfr column
			total_cfr = self.df2['fmlycfr'].sum()
			
			for key in self.finalDict.keys():
				value = total_cfr
				for k in key:
					value -= self.df2.loc[self.df2['fmly_id'] == k, 'fmlycfr'].iloc[0]

				for scen in self.finalDict[key].keys():
					#print self.finalDict[key]
					#print scen
					if value != 0:
						result = float(self.finalDict[key][scen])/value
						self.scenario_fml_val.append(result)

						if result > float(self.target):
							self.numbyden.append(result)
							self.comblen.append(len(key))
						else:
							self.numbyden.append('na')
							self.comblen.append(len(key))
					else:
						self.scenario_fml_val.append(0)
						self.numbyden.append('na')
						self.comblen.append(len(key))
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def getConnection(self):
		''' get database connection '''

		con = pymssql.connect(
					config.DATABASE_CONFIG['hostname'],
					config.DATABASE_CONFIG['username'],
					config.DATABASE_CONFIG['password'],
					config.DATABASE_CONFIG['dbname'],
					config.DATABASE_CONFIG['port'])

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
