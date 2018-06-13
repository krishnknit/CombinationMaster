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
import numpy as np
import pandas as pd
from itertools import combinations


class ComboMaster(object):

	def __init__(self):
		''' Initialise the class with required valiables '''
		self.csvPath = os.getcwd()
		self.fmlyDict = {}
		self.combDict = {}
		self.finalDict = {}
		self.scenario_fml_val = []
		self.numbyden = []
		self.comblen = []
		self.combfilter = []


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

		if len(data) <= 10:
			for i in range(len(data)):
				comb = combinations([1, 2, 3], 2)
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
						combtotal += self.df1.loc[(self.df1['scebdt'] == key) & (self.df1['fmly_id'] == c), 'def'].iloc[0]

					self.finalDict[comb] = {}
					self.finalDict[comb][self.combDict[comb]] = combtotal
					self.fdf = self.fdf.append({'combfmlyid': list(comb), 'scebdt': key, 'combtotal': combtotal}, ignore_index=True)
					#print self.finalDict
				
				## generate numerator/denomenator
				self.generateNumbyDen()

			self.fdf['scenario_fml_val'] = self.scenario_fml_val
			self.fdf['numbyden'] = self.numbyden
			self.fdf['combfilter'] = self.combfilter

			finalDf =  self.fdf.loc[(self.fdf['numbyden'] != 'na') & (self.fdf['combfilter'] != 'na'), ['combfmlyid', 'scebdt', 'numbyden']]
			print finalDf
			
			self.generateCSVFile(finalDf)
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def generateCSVFile(self, finalDf):
		''' generate final dataframe to csv file '''

		today = datetime.date.today()
		today = today.strftime("%Y-%m-%d")
		#print "TODAY: ", today
		csvFile = 'combomaster_'+today+'.csv'
		try:
			file = os.path.join(self.csvPath, csvFile)
			logging.info("creating csv file '{}' ...".format(file))
			excelDF = finalDf.to_csv(file, index=False)
		except Exception as e:
			logging.error("generateCSVFile(), e: {}".format(e))


	def generateNumbyDen(self):
		''' To generate nunerator and denomenator combination '''
		
		try:
			## get total of fmlycfr column
			total_cfr = self.df2['fmlycfr'].sum()
			totalRows = len(self.df2.index)
			localList = []
			g = globals()

			for l in range(1, totalRows):
				g['L_{0}'.format(l)] = []

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
							self.comblen.append({key: result})
							localList.append({key: result})
							for l in range(1, totalRows):
								if len(key) == l:
									g['L_' + str(l)].append(key)
						else:
							self.numbyden.append('na')
							self.comblen.append({key: 'na'})
							localList.append({key: result})
					else:
						self.scenario_fml_val.append(0)
						self.numbyden.append('na')
						self.comblen.append({key: 'na'})
						localList.append({key: result})

			combDF = pd.DataFrame(columns=['comb', 'totalval'])
			#print "LocalList: ", localList

			## create only dataframe whose cfrdefs more than passed target
			for l in range(1, totalRows): 
				if len(g['L_' + str(l)]) != 0:
					for comblist in self.comblen:
						for c in comblist.keys():
							if comblist[c] != 'na':
								combDF = combDF.append({'comb': c, 'totalval': comblist[c]}, ignore_index=True)

			minVal = combDF.min()['totalval']
			copycomblen = [ i for i in range(0, len(localList)) ]
			cnt = 0

			## find the closest cfr defs
			for comblist in localList:
				for c in comblist.keys():
					if comblist[c] != minVal:
						copycomblen[cnt] = 'na'
					else:
						copycomblen[cnt] = 'yes'
				cnt += 1

			self.combfilter.extend(copycomblen)
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
