"""
  Dave Skura, 2023
  
	Government of Canada weather data search
https://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?searchType=stnProv&timeframe=1&lstProvince=&optLimit=yearRange&StartYear=1948&EndYear=2023&Year=2023&Month=2&Day=4&selRowPerPage=25
https://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?searchType=stnProv&timeframe=1&lstProvince=&optLimit=yearRange&StartYear=1948&EndYear=2023&Year=2023&Month=2&Day=4&selRowPerPage=100&txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0&startRow=101
https://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?searchType=stnProv&timeframe=1&lstProvince=&optLimit=yearRange&StartYear=1948&EndYear=2023&Year=2023&Month=2&Day=4&selRowPerPage=100&txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0&startRow=8401

101
8401
"""

import requests
headers = {
    "content-type": "text/html"
}

class weather_station:
	def __init__(self,id, name, province,years,months,StationID):
		self.version=1.0
		self.id = id
		self.name = name
		self.province = province
		self.data_interval = ['Daily','Monthly']
		self.years = years
		self.months = months
		self.StationID = StationID

url_base = "https://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html" + "?"
url_querystring = "searchType=stnProv" + "&"
url_querystring += "timeframe=1" + "&"
url_querystring += "lstProvince=" + "&"
url_querystring += "optLimit=yearRange" + "&"
url_querystring += "StartYear=1948" + "&"
url_querystring += "EndYear=2023" + "&"
url_querystring += "Year=2023" + "&"
url_querystring += "Month=2" + "&"
url_querystring += "Day=4" + "&"
url_querystring += "selRowPerPage=25" + "&"
url_extra_querystring = "txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0" + "&"
url_extra_querystring = "startRow=" 

f = open('CanadaWeatherStations.csv','w')

for pg in range(1, 8401, 100):
	data = []
	if pg == 1:
		url = url_base + url_querystring
	else:
		url = url_base + url_querystring + url_extra_querystring + str(pg)

	print (url) # 
	response = requests.post(url, headers=headers)
	html = response.text

	start_at = 0
	while start_at > -1:
		pattern1 = '<form action="/climate_data/interform_e.html" method="post" id="'
		pattern2 = '<div class="col-lg-3 col-md-3 col-sm-3 col-xs-3">' #station name
		pattern7 = 'name="StationID" value="' # StationID

		start_at = html.find(pattern1,start_at+1)
		end_at = html.find('"',start_at + len(pattern1))
		search_id =  html[start_at + len(pattern1):end_at]
		
		station_strt = html.find(pattern2,end_at)
		if search_id != '=' and station_strt > -1:
			station_at = html.find('</div>',station_strt + len(pattern2))
			station =  html[station_strt + len(pattern2):station_at]

			StationID_start =  html.find(pattern7,start_at+1)
			StationID_at = html.find('"',StationID_start + len(pattern7))
			StationID = html[StationID_start + len(pattern7):StationID_at]

			pattern3 = '<div class="col-lg-1 col-md-1 col-sm-1 col-xs-1">' #province

			province_strt = html.find(pattern3,end_at)
			province_at = html.find('</div>',province_strt + len(pattern3))
			province = html[province_strt + len(pattern3):province_at]

			pattern4 = 'name="Year"'

			year_opt_strt = html.find(pattern4,end_at) + len(pattern4)
			year_opt_end = html.find('</select>',year_opt_strt)
			station_years = []

			pattern5 = '<option'
			pattern6 = 'name="Month"'

			while year_opt_strt > -1: # and year_opt_strt < year_opt_end:
				year_opt_strt = html.find(pattern5,year_opt_strt+1)
				if year_opt_strt > year_opt_end:
					break
				year_at = html.find('</select>',year_opt_strt )
				option_str = html[year_opt_strt + len(pattern5):year_at]
				yr_strt = option_str.find('value="') + 7
				yr_at = option_str.find('"',yr_strt)
				yr = option_str[yr_strt:yr_at]
				station_years.append(yr)

			month_opt_strt = html.find(pattern6,year_opt_end) + len(pattern6)
			month_opt_end = html.find('</select>',month_opt_strt)
			#print(html[month_opt_strt:month_opt_end])
			#break
			station_months = []
			while month_opt_strt > -1:
				month_opt_strt = html.find(pattern5,month_opt_strt+1) 
				if month_opt_strt > month_opt_end:
					break
				month_at = html.find('</select>',month_opt_strt)
				option_str = html[month_opt_strt + len(pattern5):month_opt_end]
				month_strt = option_str.find('value="') + 7
				month__at = option_str.find('"',month_strt)
				month = option_str[month_strt:month__at]
				station_months.append(month)

			data.append(weather_station(search_id,station,province,station_years,station_months,StationID))
			#break

	for i in range(0,len(data)):
		line = data[i].StationID + '\t' + 'province=' + data[i].province + '\t' + 'id=' + data[i].id + '\t station=' + data[i].name + '\t years=' + str(data[i].years) + '\t months=' + str(data[i].months) + '\n'
		f.write(line)

f.close()

