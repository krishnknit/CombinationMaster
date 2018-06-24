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
import pandas as pd
from itertools import combinations


class ComboMaster(object):

	def __init__(self):
		''' Initialise the class with required valiables '''
		self.finalCombMinDF = pd.DataFrame(columns=['combfmlyid', 'scenario_dt', 'numbyden'])
		self.csvPath = os.getcwd()
		self.fmlyDict = {}
		self.combDict = {}
		self.finalDict = {}
		self.scenario_fml_val = []
		self.numbyden = []
		self.comblen = []
		self.keyCounts = 0


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
			datelist = pd.date_range(pd.datetime.today(), periods=10).tolist()

			for business_dt in datelist:
				sqlstr_1 = '''SELECT a.bus_dt, a.fmly_id, scenario_dt, defcny_no_offset, cover1, b.fmly_cfr 
							  FROM dbo.g_cover1 a, dbo.g_fmily_cfr b
							  WHERE a.bus_dt = '{}' 
							  AND a.bus_dt = b.bus_dt 
							  AND a.fmly_id = b.fmly_id 
							  AND defcny_no_offset > 0 
							  ORDER BY scenario_dt ASC, cover1 DESC'''.format(business_dt)

				self.df1 = pd.read_sql(sqlstr_1, con)
				self.df1.sort_values(['scenario_dt', 'cover1'], ascending=[True, False])
				self.df3 = self.df1

				sqlstr_2 = '''SELECT * FROM CWA_FICC_ST_P, dbo.gsd_family_cfr 
							  WHERE bus_dt = '{}' '''.format(business_dt)
				
				self.df2 = pd.read_sql(sqlstr_2, con)
				#print self.df1
				#print self.df2
				for index, row in self.df1.iterrows():
					self.fmlyDict.setdefault(row['scenario_dt'], []).append(row['fmly_id'])

		except Exception as e:
			logging.error("readData(), e: {}".format(e))
		else:
			## close database connection
			con.close()
			self.finalCombMinDF = pd.DataFrame(columns=['combfmlyid', 'scenario_dt', 'numbyden'])
			self.fmlyDict = {}
			self.combDict = {}
			self.finalDict = {}
			self.scenario_fml_val = []
			self.numbyden = []
			self.comblen = []
			self.keyCounts = 0
			self.createCombination()


	def generateComb(self, data, kcnt):
		comb = combinations(data, kcnt)
		for cc in comb:
			yield cc

	def newGetCombinations(self, data, key, kcnt):
		''' get the combinations for correct data '''
		try:
			fdf_temp = pd.DataFrame(columns=['fmly_comb', 'scenario_dt', 'combtotal_def', 'combtotal_cfr', 'covern'])
			total_cfr = self.df2['fmly_cfr'].sum()
			genObj = self.generateComb(data, kcnt)
			for cc in genObj:
				combtotal_def = 0
				combtotal_cfr = total_cfr
				covern = 0.0
				for c in cc:
					compVal = self.df3.loc[(self.df3['scenario_dt'] == key) & (self.df3['fmly_id'] == c), 'def']
					if compVal.empty:
						continue
					combtotal_def += compVal.iloc[0, 0]
					combtotal_cfr -= compVal.iloc[0, 1]

				if combtotal_cfr != 0:
					covern = float(combtotal_def)/combtotal_cfr

				if covern - self.target < 0.01:
					self.fdf_temp = self.fdf_temp.append(
								{'combfmlyid': list(cc), 
								'scenario_dt': key, 
								'combtotal_def': combtotal_def,
								'combtotal_cfr': combtotal_cfr}, ignore_index=True)

				if len(self.fdf_temp) >= 5:
					self.fdf = self.fdf.append(fdf_temp)
					fdf_temp.drop(fdf_temp.index, inplace=True)
					break
		except Exception as e:
			logging.error("newGetCombinations(), e: {}".format(e))


	def getCombinations(self, data, key, kcnt):
		''' get the combinations for correct data '''
		try:
			combtotal = 0
			genObj = self.generateComb(data, kcnt)
			for cc in genObj:
				for c in cc:
					compVal = self.df1.loc[
								(self.df1['scenario_dt'] == key) & 
								(self.df1['fmly_id'] == c), 
								'def']
					if compVal.empty:
						continue
					combtotal += compVal.iloc[0]
				
				self.fdf = self.fdf.append(
								{'combfmlyid': list(cc), 
								'scenario_dt': key, 
								'combtotal': combtotal}, ignore_index=True)

		except Exception as e:
			logging.error("getCombinations(), e: {}".format(e))


	def createCombination(self):
		''' function to create combination all fmly_id uniquely '''

		try:
			self.fdf = pd.DataFrame(columns=['fmly_comb', 'scenario_dt', 'combtotal_def', 'combtotal_cfr'])
			self.fdf_comb = pd.DataFrame(columns=['fmly_comb'])
			self.reducedDF = pd.DataFrame(columns=['combfmlyid', 'scenarioDate', 'cover1', 'calcVal'])
						
			for key in self.fmlyDict.keys():
				keyCounts = self.getReducedFmlyIds(self.fmlyDict[key], key, self.df1	)
				self.newGetCombinations(self.fmlyDict[key], key, keyCounts + 1)

			## generate numerator/denomenator
			self.generateNumbyDen()
			#print self.fdf
			self.fdf['scenario_fml_val'] = self.scenario_fml_val
			self.fdf['numbyden'] = self.numbyden
			self.fdf['comblen'] = self.comblen

			#print self.fdf
			#finalDf =  self.fdf.loc[(self.fdf['numbyden'] != 'na') & (self.fdf['combfilter'] != 'na'), ['combfmlyid', 'scenario_dt', 'numbyden']]
			finalDf =  self.fdf.loc[(self.fdf['numbyden'] != 'na'), ['combfmlyid', 'scenario_dt', 'numbyden', 'comblen']]
			#print finalDf
			
			self.generateMinComb(finalDf)
			self.generateCSVFile()
		except Exception as e:
			logging.error("createCombination(), e: {}".format(e))


	def getReducedFmlyIds(self, fmlyIds, scen_date):
		try:
			tfid = []
			df1_tmp = pd.DataFrame()
			df1_tmp = self.df1.copy(deep=True)
			total_cfr = self.df2['fmlycfr'].sum()

			gd = self.df1.groupby('scen_date')
			for name, group in gd:
				if name == scen_date:
					df1_tmp['comb_def'] = gd.get_group(scen_date)['defcny_no_offset'].transform(pd.series.cumsum)
					df1_tmp['comb_cfr'] = total_cfr - gd.get_group(scen_date)['fmly_cfr'].transform(pd.series.cumsum)
					df1_tmp['calc_val'] = df1_tmp['comb_def']/df1_tmp['comb_cfr']
					df1_tmp.dropna(inplace=True)
					df1_tmp = df1_tmp.reset_index()
					break
		
			if df1_tmp['calc_val'].max() > self.target:
				return df1_tmp[df1_tmp.calc_val > self.target].index[0]
			else:
				return 0
		except Exception as e:
			logging.error("getReducedFmlyIds(), e: {}".format(e))


	def generateMinComb(self, finalDf):
		try:
			grouped = finalDf.groupby('scenario_dt')
			for name, group in grouped:
				combMinDF = pd.DataFrame(columns=['combfmlyid', 'scenario_dt', 'numbyden', 'comblen'])
				for index, row in finalDf.iterrows():
					if row['scenario_dt'] == name:
						combMinDF = combMinDF.append({'combfmlyid': row['combfmlyid'], 'scenario_dt': row['scenario_dt'], 'numbyden': row['numbyden'], 'comblen': row['comblen']}, ignore_index=True)
				
				gd = combMinDF.groupby('comblen')
				cnt = 0
				for n, g in gd:
				# 	print n
					minn = g.min()['numbyden']
					for i, r in finalDf.iterrows():
						if r['numbyden'] == minn:
							self.finalCombMinDF = self.finalCombMinDF.append({'combfmlyid': r['combfmlyid'], 'scenario_dt': r['scenario_dt'], 'numbyden': r['numbyden']}, ignore_index=True)
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

			for index, row in self.fdf.iterrows():
				value = total_cfr
				for k in row['combfmlyid']:				
					value -= self.df2.loc[self.df2['fmly_id'] == k, 'fmlycfr'].iloc[0]

				if value != 0:
					result = float(row['combtotal'])/value
					self.scenario_fml_val.append(result)

					if result > float(self.target):
						self.numbyden.append(result)
						self.comblen.append(len(row['combfmlyid']))
					else:
						self.numbyden.append('na')
						self.comblen.append(len(row['combfmlyid']))
				else:
					self.scenario_fml_val.append(0)
					self.numbyden.append('na')
					self.comblen.append(len(row['combfmlyid']))
		except Exception as e:
			logging.error("generateNumbyDen(), e: {}".format(e))


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
		#self.createCombination()
		etime = time.time()
		ttime = etime - stime		
		logging.info("********************************************************")
		logging.info("time consumed to complete the process: {}".format(ttime))
		logging.info("completing combo master process ...")
		logging.info("********************************************************")


if __name__ == '__main__':
	com = ComboMaster()
	com.main()
