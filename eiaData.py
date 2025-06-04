import pandas as pd
#https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?api_key=zugthYxKtQPPYNLgDt6E2PgeIs6dodssCY7XbAgw&frequency=monthly&data[0]=latitude&data[1]=longitude&data[2]=nameplate-capacity-mw&data[3]=operating-year-month&data[4]=planned-retirement-year-month&facets&[status][]=OP&start=2025-02&end=2025-02&sort[0][column]=period&sort[0][direction]=desc&sort[1][column]=nameplate-capacity-mw&sort[1][direction]=asc&offset=0&length=5000
from myeia.api import API
import os
eia = API()
os.environ["EIA_TOKEN"] = "zugthYxKtQPPYNLgDt6E2PgeIs6dodssCY7XbAgw"
df = eia.get_series_via_route(
  route="electricity/operating-generator-capacity",
  frequency="monthly")

def urlBuilder(year, month, offset=0):
    #base
    url = "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/?api_key=zugthYxKtQPPYNLgDt6E2PgeIs6dodssCY7XbAgw&frequency=monthly&data[0]=latitude&data[1]=longitude&data[2]=nameplate-capacity-mw&data[3]=operating-year-month&data[4]=planned-retirement-year-month&facets[status][]=OP"
    #date loop
    url = url + "&start=" + str(year) + "-" + str(month) + "&end=" + str(year) + "-" + str(month)
    #sort section
    url = url + "&sort[0][column]=period&sort[0][direction]=desc&sort[1][column]=nameplate-capacity-mw&sort[1][direction]=desc"
    #offset loop
    url = url + "&offset=" + str(offset) + "&length=5000"
    return(url)

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


year = '2024'
month = '10'

test = urlBuilder(year,month)

small = 10000
o =0
genData = pd.DataFrame()
while o<100 and small>50:
    url = urlBuilder(year,month,o)
    response = pd.read_json(url)
    dict = response['response']['data']
    dat = pd.DataFrame.from_dict(dict)
    small = dat['nameplate-capacity-mw'][dat.shape[0]-1]
    o += 1
    print('Smallest reactor is '+str(small)+'MW')
    print(o)
    genData = pd.concat([genData,dat],ignore_index=True)


formatedData = genData[['latitude','longitude','operating-year-month','nameplate-capacity-mw','plantid','energy-source-desc']]
formatedData = formatedData.drop_duplicates()
formatedData = formatedData.dropna(how='any',axis=0) 
gen_to_filter = ['Purchased Steam', 'Blast-Furnace Gas', 'Waste Heat', 'Other Gas','Coal-Derived Synthesis Gas','Electricity used for energy storage']
formatedData = formatedData[~formatedData['type'].isin(gen_to_filter)]
formatedData['Category'] = formatedData['type'].map(tag_generation)
formatedData = formatedData.sort_values('operating-year-month',ascending=True)
formatedData.columns = ['latitude','longitude','date','capacity','plantid','type']
formatedData.to_csv('D:/511_Project/Formated_GeneratorData.csv',index=False)

formatedData['type'].unique()
formatedData['capacity'] = formatedData['capacity'].astype(float)
formatedData['capacity'].describe()


    
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
#
#filter out -> purchased steam, blast-furnace gas, waste heat, other gas, storage, coal derived synthesis gas