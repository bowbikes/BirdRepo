import pandas as pd

#this function takes in a date, and an offset and builds a URL to query the EIA API and get opperating Electricity Generators
def urlBuilder(year, month, offset=0):
    #base
    url = "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?api_key="+YourAPIKeyHere+"zugthYxKtQPPYNLgDt6E2PgeIs6dodssCY7XbAgw&frequency=monthly&data[0]=latitude&data[1]=longitude&data[2]=nameplate-capacity-mw&data[3]=operating-year-month&data[4]=planned-retirement-year-month&facets[status][]=OP"
    #date loop
    url = url + "&start=" + str(year) + "-" + str(month) + "&end=" + str(year) + "-" + str(month)
    #sort section
    url = url + "&sort[0][column]=period&sort[0][direction]=desc&sort[1][column]=nameplate-capacity-mw&sort[1][direction]=desc"
    #offset loop
    url = url + "&offset=" + str(offset) + "&length=5000"
    return(url)

#filter out -> purchased steam, blast-furnace gas, waste heat, other gas, storage, coal derived synthesis gas
#Categories    
#Water -> hydro
#Natural gas
#contains coal
#petrolium coke -> petrolium
#DFO -> petrolium
#RFO -> petrolium
#lignite -> coal
#WWS -> biofuel
#JF -> petrolium
#Kerosene -> petrolium
#MSW -> biofuel
#black liquor -> biofuel
#Ag Byproduc -> biofuel

#this function takes in a generator's type as a string and categorizes that type of generation into one of 9 categories
def tag_generation(gen_type):
    if 'coal' in gen_type.lower():
      return 'Coal'
    elif gen_type in ['Natural Gas', 'Nuclear', 'Geothermal','Wind', 'Solar']:
        return(gen_type)
    elif gen_type in ['Petroleum Coke', 'Disillate Fuel Oil', 'Residual Fuel Oil', 'Jet Fuel', 'Kerosene']:
        return('Petroleum')
    elif gen_type == 'Lignite':
        return 'Coal'
    elif gen_type == "Water":
        return "Hydro"
    else:
        return 'Biofuel'

#pull annual data since 1960
#use string builder to find op all opperation power plants sorted by nameplate capactiy desc
#add df to larger dataframe
#loop through one at a time till you have 10,000 plants or till lowest capacity <5MW
year = 2025 #most recent date for current data
month = 10
small = 10000
o = 0
genData = pd.DataFrame()
#loop until there have been 100 offests(each offset is 5000 rows of data from the EIA)
#or until the smallest generator we see is 50MW(the power needs for a small community)
while o<100 and small>50:
    #build a url to get generator data
    url = urlBuilder(year,month,o)
    #get the web's response for that url as a json file
    response = pd.read_json(url)
    #grab the data from the web response
    dict = response['response']['data']
    #convert json data to a pandas DataFrame
    dat = pd.DataFrame.from_dict(dict)
    #update itterators
    small = dat['nameplate-capacity-mw'][dat.shape[0]-1]
    o += 1
    #add data to the meta dataframe
    genData = pd.concat([genData,dat],ignore_index=True)

#format the data so it can be digested by observablehq
formatedData = genData[['latitude','longitude','operating-year-month','nameplate-capacity-mw','plantid','energy-source-desc']]
#clean any duplicate rows or rows with nonexistent data
formatedData = formatedData.drop_duplicates()
formatedData = formatedData.dropna(how='any',axis=0) 
#filter out irrelevant modes of electricity generation
gen_to_filter = ['Purchased Steam', 'Blast-Furnace Gas', 'Waste Heat', 'Other Gas','Coal-Derived Synthesis Gas','Electricity used for energy storage']
formatedData = formatedData[~formatedData['type'].isin(gen_to_filter)]
#categorize the electrical generation into the 9 core types
formatedData['Category'] = formatedData['type'].map(tag_generation)
#sort values to make data processing down the line easier
formatedData = formatedData.sort_values('operating-year-month',ascending=True)
#Column header formatting
formatedData.columns = ['latitude','longitude','date','capacity','plantid','type']
#write the file out for use elsewhere.
formatedData.to_csv('D:/511_Project/Formated_GeneratorData.csv',index=False)
