#  Dave Skura, May 17,2019
import sys
import numpy

class resultset():
	def __init__(self,dbcursor):
		self.dm = davemath()
		self.dm.addhdrdata(dbcursor.description,dbcursor.fetchall())
	def printhdr(self):
		self.dm.printhdr()
	def printdata(self):
		self.dm.printdata()
	def printit(self):
		if self.dm.header_row[0].lower() !='msg':
			self.dm.printhdr()
		self.dm.printdata()
	def fileit(self,szfilename):
		if self.dm.header_row[0].lower() !='msg':
			self.dm.filehdr(szfilename)
		self.dm.filedata(szfilename)


class avgline():
	def __init__(self,label,field_nbr,dataset):
		self.label		= label
		self.field_nbr	= int(field_nbr)
		self.data_set	= dataset
		self.rowcount	= len(self.data_set)
		self.avgvalue	= 0.0
		self.getaverage()

	def getaverage(self):
		total = 0.0
		for r in self.data_set:
			total += float(r[self.field_nbr])
		self.avgvalue = round(float(total/self.rowcount),3)

class trend():
	def __init__(self,label,field_nbr,dataset):
		self.avgvalue	= 0.0
		self.avg_slope	= 1
		self.label		= label
		self.field_nbr	= int(field_nbr)
		self.data_set	= dataset
		self.rowcount	= len(self.data_set)
		self.determine_avg_slope()
		self.getaverage()
		self.uplift = self.findtrendline_uplift()

	def getaverage(self):
		total = 0.0
		for r in self.data_set:
			total += float(r[self.field_nbr])
		self.avgvalue = round(float(total/self.rowcount),3)

	def vmin(self,col):
		val = None
		for x in range(0,self.rowcount):
			if val is None:
				val = self.data_set[x][col]
			else:
				if float(val) > float(self.data_set[x][col]):
					val = self.data_set[x][col]
		return val

	def vmax(self,col):
		val = None
		for x in range(0,self.rowcount):
			if val is None:
				val = self.data_set[x][col]
			else:
				if float(val) < float(self.data_set[x][col]):
					val = self.data_set[x][col]
		return val

	def findtrendline_uplift(self):
		#print(self.avgvalue)
		#print(self.rowcount)
		#print(self.field_nbr)
		#print(self.avg_slope)

		#print(self.data_set[int(self.rowcount/2)][self.field_nbr])
		#print(self.avgvalue - int(self.rowcount/2)*self.avg_slope)
		#sys.exit(1)


		return float(self.avgvalue - int(self.rowcount/2)*self.avg_slope)


	def findtrendline_uplift_prv(self):
		lowestdiff_i = None
		differences = {}
		istart = int(self.vmin(self.field_nbr))
		if istart == 0: istart=1 
		iend = int(self.vmax(self.field_nbr))
		if iend == 0: iend=1 
		istep = int((iend-istart)/100)+1
		for i in range(istart,iend,istep):
			fcter = i
			trial = []
			for x in range(0,self.rowcount):
				trial.append(round(float(x)*float(self.avg_slope),4)+fcter)
			totaldiff = 0.0000
			for x in range(0,self.rowcount):
				# compare y value in spot [1] ... to trial value for possible trend
				totaldiff += abs(float(self.data_set[x][self.field_nbr])-float(trial[x]))

			differences[fcter] = totaldiff

			if lowestdiff_i == None:
				lowestdiff_i = fcter
			else:
				if float(differences[lowestdiff_i]) > float(totaldiff):
					lowestdiff_i = fcter

		return lowestdiff_i

	def determine_avg_slope(self):
		slopes = []
		SlopeTotal = 0.0000
		for x1 in range(0,self.rowcount):
			pt1_x = x1	
			pt1_y = self.data_set[x1][1]	
			if x1 + 1 < self.rowcount:
				x2 = x1 + 1
				pt2_x = x2	
				pt2_y = self.data_set[x2][1]	
				thisslope = self.calc_slope(pt1_x,pt1_y,pt2_x,pt2_y)		
				slopes.append(thisslope)
				SlopeTotal += thisslope
				#print('point(x,y) ' + str(pt1_x) + ',' + str(pt1_y) + ' compared to ' + str(pt2_x) + ',' + str(pt2_y) + ' is ' + str(thisslope) + '\n')
	

		self.avg_slope = round(float(SlopeTotal)/float(len(slopes)),4)
		#print('avg_slope ' + str(self.avg_slope) )

	def calc_slope(self,x1=1,y1=1,x2=1,y2=1):
		if float(x2) == float(x1):
			slope = 1
		else:
			slope =  (float(y2)-float(y1)) / (float(x2)-float(x1))
		return slope

class davemath():
	def __init__(self):
		self.header_row		= []
		self.data_set		= []
		self.maxlength		= []
		self.tighten		= 0
		self.gap			= 0
		self.uplift			= 0
		self.lower_uplift	= 0
		self.upper_uplift	= 0
		self.trendlines		= []
		self.rowcount		= 0
		self.trends			= []
		
	
	def hidecols(self,hidecols):

		cols_to_hide = hidecols.split(',') 
		hdr = []
		keeplist = []
		i = 0
		for j in range(0,len(self.header_row)):
			if not self.header_row[j] in cols_to_hide:
				hdr.append(self.header_row[j])
				keeplist.append(i)
			i += 1

		self.header_row=hdr


		data = []
		for row in self.data_set:
			lst = list(row)
			lst2 = list()

			for i in range(0,len(lst)):
				if i in keeplist:
					lst2.append(lst[i])
						
			data.append(tuple(lst2))


		self.data_set = data

		self.col_count	= len(self.header_row)

		self.checklens()


	def repeatchar(self,s, wanted):
		return s * (wanted//len(s) + 1)

	def addhdrdata(self,cursor_fields,data_set):
		for k in [i[0] for i in cursor_fields]:
			self.header_row.append(k)

		self.col_count	= len(self.header_row)
		self.data_set	= data_set
		self.rowcount	= len(self.data_set)
		self.checklens()

	def addavgs(self,requested_avgs):
		self.avgs = []
		for avg_request in requested_avgs:
			label = self.header_row[int(avg_request)] + '-avg'
			new_avg = avgline(label,int(avg_request),self.data_set)
			self.header_row.append(new_avg.label)
			self.avgs.append(new_avg)

		data = []
		for row in self.data_set:
			lst = list(row)
			for one_avg in self.avgs:
				lst.append(one_avg.avgvalue)
			
			data.append(tuple(lst))


		self.data_set = data
		self.col_count	= len(self.header_row)

		self.checklens()


	def addtrendlines(self,requested_trendlines):
		self.trendlines = []

		for trend_request in requested_trendlines:
			label = self.header_row[int(trend_request)] + '-trend'
			new_trend = trend(label,int(trend_request),self.data_set)
			self.header_row.append(new_trend.label)
			self.trendlines.append(new_trend)

		data = []
		x = 0
		for row in self.data_set:
			lst = list(row)
			for one_trend in self.trendlines:
				lst.append(round(float(x)*one_trend.avg_slope+one_trend.uplift,4))
			
			data.append(tuple(lst))
			x += 1

		self.data_set = data
		self.col_count	= len(self.header_row)

		self.checklens()

	def checklens(self):	
		self.maxlength	= []
		for k in range(0,self.col_count):
			self.maxlength.append(len(self.header_row[k]))

		for row in self.data_set:
			for i in range(0,self.col_count):
				if len(str(row[i])) > int(self.maxlength[i]):
					self.maxlength[i] = len(str(row[i]))
	
	def filehdr(self,szfilename):
		R = ''
		l = ''
		for i in range(0,self.col_count):
			k = self.header_row[i]
			R += k + self.repeatchar(' ',(int(self.maxlength[i]) - len(k.strip())) + 1)
			l += self.repeatchar('-',int(self.maxlength[i])) + ' '

		f = open(szfilename,'a')
		f.write(R + '\n' + l + '\n')
		f.close()

	def printhdr(self):
		R = ''
		l = ''
		for i in range(0,self.col_count):
			k = self.header_row[i]
			R += k + self.repeatchar(' ',(int(self.maxlength[i]) - len(k.strip())) + 1)
			l += self.repeatchar('-',int(self.maxlength[i])) + ' '

		print(R + '\n' + l + '\n')

	def filedata(self,szfilename):
		R = ''
		f = open(szfilename,'a')
		for row in self.data_set:							
			line = ''
			for i in range(0,self.col_count):
				line += str(row[i]) + self.repeatchar(' ',self.maxlength[i]-len(str(row[i])) + 1) 
			f.write(line + '\n')
		f.close()


	def printdata(self):
		R = ''
		for row in self.data_set:							
			line = ''
			for i in range(0,self.col_count):
				line += str(row[i]) + self.repeatchar(' ',self.maxlength[i]-len(str(row[i])) + 1) 

			print(line + '\n')

