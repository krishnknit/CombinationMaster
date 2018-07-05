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
import getopt
import pandas as pd
import numpy as np
from itertools import combinations


class ComboMaster(object):

	def __init__(self):
		''' Initialise the class with required valiables '''

		self.finalCombMinDF = pd.DataFrame(columns=['combfmlyid', 'scenario_dt', 'numbyden'])
		self.fdf_display=pd.DataFrame(columns=['bus_dt','fmly_comb','scenario_dt','combtotal_def','combtotal_cfr','covern_multdef'])
		self.printingDF=pd.DataFrame(columns=['bus_dt','fmly_id','scenario_dt','defcny_nooffset','cover1','fmly_cfr','comb_def','comb_cfr','calc_val','num_fmly'])
		self.df_table_insert=pd.DataFrame(columns=['bus_dt', 'scenario_dt', 'fmly_id', 'lastupdates'])
		self.csvPath = os.getcwd()
		self.fmlyDict = {}
		self.combDict = {}
		self.finalDict = {}
		self.scenario_fml_val = []
		self.numbyden = []
		self.comblen = []
		self.keyCounts = 0
		self.target=0.5
		self.final_target=1.0
		self.start_bus_dt='20180101'
		self.start_bus_dt='20180102'
		self.comb_len=[]
		self.comb_fltr=[]
		self.dist_bus_dt=[]		


	def getargs(self):
		''' Read passed command line arguments '''

		parser = argparse.ArgumentParser()
		parser.add_argument( '--sdate', '-s', type=str, required=True, help='start date')
		parser.add_argument('--edate', '-e', type=str, required=True, help='end date')
		parser.add_argument('--target', '-t', type=float, required=True, help='target')
		parser.add_argument('--finaltarget', '-f', type=float, required=True, help='final target')

		args = parser.parse_args()

		if args.target > 1:
		    print ("target should not greater than 1")

		if args.ftarget > 1:
		    print ("target should not greater than 1")


		# if len (sys.argv) != 2 :
		# 	print ("Usage: python <SCRIPT_NAME> <TARGET> ")
		# 	sys.exit (1)
		# else:
		# 	self.target = sys.argv[1]


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
				self.fmlyDict.clear()
				for index, row in self.df1.iterrows():
					self.fmlyDict.setdefault(row['scenario_dt'], []).append(row['fmly_id'])

		except Exception as e:
			logging.error("readData(), e: {}".format(e))
		else:
			## close database connection


	def newGetCombinations(self, data, key, kcnt,bus_dt):
		''' get the combinations for correct data '''
		try:
			fdf_temp = pd.DataFrame(columns=['bus_dt','fmly_comb', 'scenario_dt', 'combtotal_def', 'combtotal_cfr', 'covern_multdef'])
			total_cfr = self.df2['fmly_cfr'].sum()
			combtotal_def = 0
			combtotal_cfr = total_cfr
			covern_multdef = 0.0
			for c in data:
				compVal = self.df3.loc[(self.df3['scenario_dt'] == key) & (self.df3['fmly_id'] == c), ['def',fmly_cfr]]
				if compVal.empty:
					continue
				combtotal_def += compVal.iloc[0, 0]
				combtotal_cfr -= compVal.iloc[0, 1]

			if combtotal_cfr != 0:
				covern_multdef = float(combtotal_def)/combtotal_cfr

			if covern_multdef > self.target:
				self.fdf = self.fdf.append(
							{'bus_dt':bus_dt
							'fmly_comb': list(cc), 
							'scenario_dt': key, 
							'combtotal_def': combtotal_def,
							'combtotal_cfr': combtotal_cfr, 
							'covernmult_def':covern_multdef}, ignore_index=True)

			self.fdf.sort_values(['covern_multdef'],ascending=[False],inplace=True)
			self.fdf=self.fdf.iloc[0]	
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
			self.fdf = pd.DataFrame(columns=['fmly_comb', 'scenario_dt', 'combtotal_def', 'combtotal_cfr','covern_multdef'])
			self.fdf_comb = pd.DataFrame(columns=['fmly_comb'])
			self.reducedDF = pd.DataFrame(columns=['combfmlyid', 'scenarioDate', 'cover1', 'calcVal'])
			temp_list=[]
						
			for key in self.fmlyDict.keys():
				keyCounts, fmlyIds, temp_scenario_date = self.getReducedFmlyIds(self.fmlyDict[key], key)
				if keyCounts!=0:
					temp_list.append(tuple([keyCounts,temp_scenario_date]))
					min_comb,min_scenario_dt=min(temp_list,key=lambda item:item[0])
                        
				if min_comb!=0:
					self.newgetCombinations(fmlyIds,min_scenario_dt,min_comb+1,bus_dt)

			self.fdf_display=self.fdf_display.appennd(self.fdf,ignore_index=True)


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
					df1_tmp['comb_fml'] = gd.get_group(scen_date)['fmly_id'].apply(lambda x: str(x) + ' ').cumsum()
					df1_tmp.dropna(inplace=True)
					df1_tmp = df1_tmp.reset_index()
					break
		
			if df1_tmp['calc_val'].max() > self.target:
				fmlid = df1_tmp[df1_tmp.calc_val > self.target]['comb_fml'].values[-1].strip().split(' ')
				fmllength = df1_tmp[df1_tmp.calc_val > self.target].index[0]
				return fmllength, fmlid, scen_date
			else:
				return 0,'9999-99-99'
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


	def createDataToExcel(self):
		self.stored_df = stored_df # get dataframe by running the stored procedure
		result = pd.merge(self.stored_df, self.fdf, on='bus_dt')
		result['covernfromother'] = result.apply (lambda row: row.total_covern - row.covern_multipledef, axis=1)
		result = result.reindex(columns=['bus_dt', 'scenario_dt', 'fmly_comb', 'total_covern', 'covern_multipledef', 'covernfromother', 'shk', 'mpler'])
		
		logging.info("creating excel file ...")
		excelFile = os.path.join(os.getcwd(), 'excel_report.xlsx')

		try:
			writer = pd.ExcelWriter(excelFile, engine='xlsxwriter')
			result.to_excel(writer, sheet_name='final df', index=False)
			self.stored_df.to_excel(writer, sheet_name='stored df', index=False)
			self.fdf.to_excel(writer, sheet_name='fdf', index=False)
			writer.save()
		except Exception as e:
			logging.error("createDataToExcel(): unable to write excel file, e: {}".format(e))
		else:
			logging.info("excel file: {} has created ...".format(excelFile))
        

	def exec_stored_proc(self):
		con,cur=getconnection()
		sql_exec=''' exec p_reverse ? ?;'''
		values=(20180101,20180102)
		cur.execute(sql_exec,values)
		cur.commit
		con.close


	def writeData_conn(self):
		engine:sqlachemy.engine.base.Emgine=None
		conn.sqlachemy.engine.connection=None
		params = urllib.parse.quote_plus(conn_str)
		engine = create_engine("mssql+pyodbc://?odbc_connect=%s" % params)
		conn = engine.connect()
		try:
			res = conn.execute('''SELECT COUNT(*) 
								  FROM gsd_scenario_famly 
								  WHERE bus_dt 
								  BETWEEN '{}' AND '{}' '''.format(start_dt, end_dt))
			if res:
				conn.execute('''DELETE FROM gsd_scenario_famly 
								WHERE bus_dt 
								BETWEEN '{}' AND '{}' '''.format(start_dt, end_dt))

			self.df_table_insert.to_sql(name='gsd_scenario_famly', con=conn, schema='dbo', if_exists='append', index=False)
		except Exception as e:
			logging.error("writeData_conn(): insertion failed, e: {}".format(e))
			conn.close()


	def gatherData():
		temp_dates=pd.DataFrame()
		con,cur=getconnection()
		sqlstr_bus_dt = '''	SELECT DISTINCT bus_dt 
							FROM g_cover1 
							WHERE bus_dt 
							BETWEEN {} AND {} ORDER BY bus_dt ASC
						'''.format(self.start_bus_dt,self.end_bus_dt)
		try:
			temp_dates=read_sql(sqlstr_bus_dt,con)
			dist_bus_dt=temp_dates.iloc[:,0].tolist()
			for bus_dt in dist_bus_dt:
				self.readData(bus_dt)
				self.createcombination(bus_dt)
		except Exception as e:
			logging.error("gatherData(): unable to gather data, e: {}".format(e))


	def main(self):
		logging.basicConfig(filename=os.path.join(os.getcwd(), 'comboMaster.log'),
							format='%(asctime)s: %(levelname)s: %(message)s',
							level=logging.DEBUG)

		logging.info("********************************************************")
		logging.info("starting combo master process ...")
		logging.info("********************************************************")
		stime = time.time()
		self.getargs()
		self.gatherData()
		for index,row in self.fdf_display.iterrows():
			for id in row['fmly_comb']:
				self.df_table_insert=self.df_table_insert.append({'bus_dt':row['bus_dt'],'scenario_dt':eow['scenario_dt'],'fmly_id':id})
		
		self.exec_stored_proc()
		self.writedata_conn()
		self.createDataToExcel()
		etime = time.time()
		ttime = etime - stime		
		logging.info("********************************************************")
		logging.info("time consumed to complete the process: {}".format(ttime))
		logging.info("completing combo master process ...")
		logging.info("********************************************************")


if __name__ == '__main__':
	com = ComboMaster()
	com.main()
