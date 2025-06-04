#DTW  & DBSCAN 
import pandas as pd
import numpy as np
import math
import networkx as nx
import matplotlib.pyplot as plt
import helperFunctions as hf
from dateutil.relativedelta import relativedelta
import json


#getting data into pandas
#this is the output from a sql query that pulls from our azure database
df = pd.read_csv('D:/511_Project/FinalQueryResults.csv')
#just checking to make sure we dont have any duplicate data
df = df.drop_duplicates()
#converting datatypes
df['EVENT_DATE']= pd.to_datetime(df['EVENT_DATE'],format='%m/%d/%Y')
#thresholds used to define the flocks (if a bird is seen within a few miles of another bird within a few days of each other then we can say theyre friends)
time_threshold = 2 #days
dist_threshold = 5 #miles


#tag flocks for each bird species
#2040 = Whooping Crane; 3490 = Golden Eagle; 1940 = Blue Heron; 1720 = canada goose
for s in [2040,3490,1940]:
    #specifiy which species we are looking at
    #filter all the query data to just the species we are looking at
    dat = df[df['SPECIES_ID']==s]
    dat = dat.sort_values(by=['EVENT_DATE'])
    #get a list of all the unique bird of that species
    birds = list(dat['ORIGINAL_BAND'].unique())
    friends = []
    print('starting on species code ' + str(s))
    #for each bird
    for b in birds:
        #for each sighting
        bird_dat = dat[dat['ORIGINAL_BAND']==b]
        events = list(bird_dat['EVENT_DATE'].unique())
        for e in events:
            #get other events around that time (plus or minus the time threshold)
            event_dat = dat[(dat['EVENT_DATE'] > (e - pd.Timedelta(days=time_threshold))) & (dat['EVENT_DATE'] <= (e + pd.Timedelta(days=time_threshold)))]
            #extract the original coordinates for this sighting event
            orig_coords = (event_dat[(event_dat['ORIGINAL_BAND']==b)&(event_dat['EVENT_DATE']==e)].iloc[0,3],event_dat[(event_dat['ORIGINAL_BAND']==b)&(event_dat['EVENT_DATE']==e)].iloc[0,4])
            #see if any of those events are close to this event, but ignore the event in question because we dont need to double count it
            event_dat = event_dat[event_dat['ORIGINAL_BAND']!=b]
            for i in range(0,event_dat.shape[0]-1):
                test_coords = (event_dat.iloc[i,3],event_dat.iloc[i,4])
                #create link between two birds
                if hf.areSameFlock(orig_coords,test_coords,dist_threshold):
                    friends.append([b,event_dat.iloc[i,1]])
    #look at link lists to try to determind flocks
    print('finished tagging each bird of this species')
    #turn all the links between birds into a new dataframe while swapping the order of any birds to make deduplicating possible next line
    flock_df = pd.DataFrame(hf.reorder(friends), columns=['from', 'to'])
    flock_df = flock_df.drop_duplicates()
    #generate a network from all the links
    G = nx.from_pandas_edgelist(flock_df, source='from', target='to')
        #graph the network for visual exploration(no longer needed while processing data)
        #nx.draw(G)
        #plt.show()
    #identify and pull out distinct clusters that emerge when generating the network.
    #any individual birds are part of flock 0
    groups = [c for c in sorted(nx.connected_components(G), key=len, reverse=True)]
    print('Generated all the flocks')
    #write processed data out to distinct files
    if s == 1720:
        fldr = 'Canada_Goose'
    elif s == 2040:
        fldr = 'Whooping_Crane'
    elif s == 3490:
        fldr = 'Golden_eagle'
    elif s == 1940:
        fldr = 'Blue_Heron'
    print('Exporting Network Data')
    flock_df.to_json('D:/511_Project/Processed_Data/'+fldr+'/edgeList.json')    
    print('Tagging flocks')
    #adds the generated flock data to the original data frame 
    dat['flock'] = dat.apply(lambda row: hf.getFlockNumber(row.ORIGINAL_BAND,groups), axis=1)
    #check for and remove duplicates again because I'm paranoid
    dat = dat.drop_duplicates()
    print('Exporting Flock Data')
    dat.to_csv('D:/511_Project/Processed_Data/'+fldr+'/flockTest.csv',index = False)


#clear any large variable
del df, dat, flock_df,birds,friends,groups

# read in EIA generation Data this was processed using their API in the eiaData.py file
EIA_df = pd.read_csv('D:/511_Project/Formated_GeneratorData.csv')
# filter to generator types that would have a plausible effect on birds
EIA_df = EIA_df[EIA_df['Category'].isin(['Hydro','Coal','Petroleum','Wind'])]
EIA_df['date']= pd.to_datetime(EIA_df['date'],format='%Y-%m')
#variable and DataFrame initialization
nodeId = 0
nodeCols = ['record_id','species','flock','nbirds','latitude','longitude','date','isoCountry']
#edgeCols = ['species','flock','source','target','totalDistance','ewDistance','nsDistance','heading','fossilCount','windCount','hydroCount','borderCount']
nodes = pd.DataFrame(columns = nodeCols)
#edges = pd.DataFrame(columns = edgeCols)
edgeJsonCols = ['species','flock','date','coords','fossilCount','windCount','borderCount','hydroCount','distanceCount','nbirds']
edges_json = pd.DataFrame(columns = edgeJsonCols)
#this loop condensces all geospacial data into a pretty line for graphing
for s in ['Whooping_Crane','Golden_eagle','Blue_Heron']:#,'Canada_Goose']:#    'Canada_Goose',
    #Read in relevant bird sighting data
    fp = 'D:/511_Project/Processed_Data/'+s+'/flockTest.csv'
    df = pd.read_csv(fp)
    #convert to datetime for utility
    df['EVENT_DATE']= pd.to_datetime(df['EVENT_DATE'],format='%Y-%m-%d')
    for f in list(df['flock'].unique()):
        #when looking at a flock we need to get the geospacial midpoint of any sightings from that flock
        if f>0:
            #inits - to be used once looking at flight segments
            nbirds = df[df['flock'] == f]['ORIGINAL_BAND'].nunique()
            lastKey = 0
            lastCountry = 'US'
            lastPoint = (0,0)
            #filter data to just the flock in question
            flock_dat = df[df['flock']==f]
            #get the first time that flock was seen
            year,month = min(flock_dat['EVENT_DATE']).year,min(flock_dat['EVENT_DATE']).month
            date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
            while date <= max(flock_dat['EVENT_DATE']):
                FossilCounter = 0
                WindCounter = 0
                HydroCounter = 0
                BorderCounter = 0
                month_dat = flock_dat[(flock_dat['EVENT_DATE'].dt.year==year) & (flock_dat['EVENT_DATE'].dt.month==month)]
                #weighted avg by more prevalent birds? # http://www.geomidpoint.com/calculation.html
                points = list(zip(month_dat['LAT_DD'], month_dat['LON_DD'], month_dat['SIGHTINGS']))
                #this is in a try statement in order to prevent an error if there were no sightings in a given month
                try:
                    mid_lat, mid_lon =  hf.geographic_midpoint(points)
                except ValueError:
                    #if there were no sightings add that to the logs and increment the month, then skip(continue) the rest of the loop and try again with the new month
                    #print(s + ' flock #' + str(f) + 'has no observations in ' +str(month)+'/'+str(year))
                    if month == 12:
                        year += 1
                        month = 1
                    else:
                        month += 1 
                    date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
                    continue
                #get the country that appears the most out of all the sightings for that month
                #this isn't perfect and can definitely be improved, but it is efficient
                country = month_dat['ISO_COUNTRY'].value_counts().idxmax()
                #this is now replacing RECORD_ID when combining multiple sightings during one month
                key = nodeId
                nodeId += 1
                #log point data: nodeid, species, flock, n-birds, lat, lon, date
                node_row = [key,s,f,month_dat.shape[0],mid_lat,mid_lon,date,country]
                #append rows to data_frames
                nodes = pd.concat([nodes, pd.DataFrame(columns = nodeCols,data = [node_row])], ignore_index=True)
                if lastPoint != (0,0):
                    #calculate the haversine distances between the last point and the current one.
                    #decompone to understand the directional components and the direction of travel
                    total_dist, ew_dist, ns_dist = hf.distance_components(lastPoint[0],lastPoint[1],mid_lat,mid_lon)
                    heading = hf.bearing_degrees(lastPoint[0],lastPoint[1],mid_lat,mid_lon)
                    #filter Generator dataset to only look at relevant generators
                    mean_lat = np.deg2rad((lastPoint[0] + mid_lat) / 2)
                    # ~110.574 km per degree latitude
                    lon_deg_per_km = 1 / (111.320 * np.cos(mean_lat))
                    lat_buf = 10 / 110.574
                    lon_buf = 10 * lon_deg_per_km
                    # build simple rectangle
                    lat_min = min(lastPoint[0], mid_lat) - lat_buf
                    lat_max = max(lastPoint[0], mid_lat) + lat_buf
                    lon_min = min(lastPoint[1], mid_lon) - lon_buf
                    lon_max = max(lastPoint[1], mid_lon) + lon_buf
                    #create a mask to be used to filter the generator data
                    rect_mask = (
                        EIA_df['latitude'].between(lat_min, lat_max) & EIA_df['longitude'].between(lon_min, lon_max)
                    )
                    #filter generators by 
                    df_rect = EIA_df[rect_mask].copy()
                    #filter out generator not built yet
                    df_rect = df_rect[df_rect['date']<=date]
                    #for all the remaining generators check their position relative to the flight path
                    for g in range(0,df_rect.shape[0]-1):
                        gen_lat = df_rect.iloc[g][0]
                        gen_lon = df_rect.iloc[g][1]
                        #this function sees if any are within 10km of the start position, end position, or along the path between and increments the approriate counter based on the type
                        if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<10.1 and (df_rect.iloc[g][6] == 'Coal' or df_rect.iloc[g][6] == 'Petroleum')):
                            FossilCounter += 1
                        if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<20.1 and (df_rect.iloc[g][6] == 'Wind')):
                            WindCounter += 1
                    if lastCountry!=country or month_dat['ISO_COUNTRY'].nunique()>1:
                        BorderCounter += 1
                    #log edge data: species, flock, source, target, distance, ew_dist,ns_dist, heading, fossil fuel counter, wind farm counter, border_crossing counter
                    #edge_row = [s,f,lastKey,key,total_dist,ew_dist,ns_dist,heading,FossilCounter,WindCounter,HydroCounter,BorderCounter]
                    edge_row_json = [s,f,date - pd.DateOffset(months=1),[[lastPoint[1],lastPoint[0]],[mid_lon,mid_lat]],FossilCounter,WindCounter,BorderCounter,HydroCounter,total_dist,nbirds]
                    #append rows to data_frames
                    #edges = pd.concat([edges, pd.DataFrame(columns = edgeCols,data = [edge_row])], ignore_index=True)
                    edges_json = pd.concat([edges_json, pd.DataFrame(columns = edgeJsonCols,data = [edge_row_json])], ignore_index=True)                      
                #increment data
                lastKey = key
                lastPoint = (mid_lat,mid_lon)
                lastCountry = country 
                if month == 12:
                    year += 1
                    month = 1
                else:
                    month += 1
                date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
        else:
            for b in list(df['ORIGINAL_BAND'].unique()):
                #inits - to be used once looking at flight segments
                lastCountry = 'US'
                lastPoint = (0,0)
                #filter data to just the flock in question
                flock_dat = df[df['ORIGINAL_BAND']==b]
                #get the first time that flock was seen
                year,month = min(flock_dat['EVENT_DATE']).year,min(flock_dat['EVENT_DATE']).month
                date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
                while date <= max(flock_dat['EVENT_DATE']):
                    FossilCounter = 0
                    WindCounter = 0
                    HydroCounter = 0
                    BorderCounter = 0
                    lastKey = 0
                    month_dat = flock_dat[(flock_dat['EVENT_DATE'].dt.year==year) & (flock_dat['EVENT_DATE'].dt.month==month)]
                    #weighted avg by more prevalent birds? # http://www.geomidpoint.com/calculation.html
                    points = list(zip(month_dat['LAT_DD'], month_dat['LON_DD'], month_dat['SIGHTINGS']))
                    #this is in a try statement in order to prevent an error if there were no sightings in a given month
                    try:
                        mid_lat, mid_lon =  hf.geographic_midpoint(points)
                    except ValueError:
                        #if there were no sightings add that to the logs and increment the month, then skip(continue) the rest of the loop and try again with the new month
                        #print(s + ' flock #' + str(f) + 'has no observations in ' +str(month)+'/'+str(year))
                        if month == 12:
                            year += 1
                            month = 1
                        else:
                            month += 1 
                        date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
                        continue
                    #get the country that appears the most out of all the sightings for that month
                    #this isn't perfect and can definitely be improved, but it is efficient
                    country = month_dat['ISO_COUNTRY'].value_counts().idxmax()
                    #this is now replacing RECORD_ID when combining multiple sightings during one month
                    key = nodeId
                    nodeId += 1
                    #log point data: nodeid, species, flock, n-birds, lat, lon, date
                    node_row = [key,s,f,month_dat.shape[0],mid_lat,mid_lon,date,country]
                    #append rows to data_frames
                    nodes = pd.concat([nodes, pd.DataFrame(columns = nodeCols,data = [node_row])], ignore_index=True)
                    if lastPoint != (0,0):
                        #calculate the haversine distances between the last point and the current one.
                        #decompone to understand the directional components and the direction of travel
                        total_dist, ew_dist, ns_dist = hf.distance_components(lastPoint[0],lastPoint[1],mid_lat,mid_lon)
                        heading = hf.bearing_degrees(lastPoint[0],lastPoint[1],mid_lat,mid_lon)
                        #filter Generator dataset to only look at relevant generators
                        mean_lat = np.deg2rad((lastPoint[0] + mid_lat) / 2)
                        # ~110.574 km per degree latitude
                        lon_deg_per_km = 1 / (111.320 * np.cos(mean_lat))
                        lat_buf = 10 / 110.574
                        lon_buf = 10 * lon_deg_per_km
                        # build simple rectangle
                        lat_min = min(lastPoint[0], mid_lat) - lat_buf
                        lat_max = max(lastPoint[0], mid_lat) + lat_buf
                        lon_min = min(lastPoint[1], mid_lon) - lon_buf
                        lon_max = max(lastPoint[1], mid_lon) + lon_buf
                        #create a mask to be used to filter the generator data
                        rect_mask = (
                            EIA_df['latitude'].between(lat_min, lat_max) & EIA_df['longitude'].between(lon_min, lon_max)
                        )
                        #filter generators by 
                        df_rect = EIA_df[rect_mask].copy()
                        #filter out generator not built yet
                        df_rect = df_rect[df_rect['date']<=date]
                        #for all the remaining generators check their position relative to the flight path
                        for g in range(0,df_rect.shape[0]-1):
                            gen_lat = df_rect.iloc[g][0]
                            gen_lon = df_rect.iloc[g][1]
                            #this function sees if any are within 10km of the start position, end position, or along the path between and increments the approriate counter based on the type
                            if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<10.1 and (df_rect.iloc[g][6] == 'Coal' or df_rect.iloc[g][6] == 'Petroleum')):
                                FossilCounter += 1
                            if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<20.1 and (df_rect.iloc[g][6] == 'Wind')):
                                WindCounter += 1
                        if lastCountry!=country or month_dat['ISO_COUNTRY'].nunique()>1:
                            BorderCounter += 1
                        #log edge data: species, flock, source, target, distance, ew_dist,ns_dist, heading, fossil fuel counter, wind farm counter, border_crossing counter
                        #edge_row = [s,f,lastKey,key,total_dist,ew_dist,ns_dist,heading,FossilCounter,WindCounter,HydroCounter,BorderCounter]
                        edge_row_json = [s,b,date - pd.DateOffset(months=1),[[lastPoint[1],lastPoint[0]],[mid_lon,mid_lat]],FossilCounter,WindCounter,BorderCounter,HydroCounter,total_dist,1]
                        #append rows to data_frames
                        #edges = pd.concat([edges, pd.DataFrame(columns = edgeCols,data = [edge_row])], ignore_index=True)
                        edges_json = pd.concat([edges_json, pd.DataFrame(columns = edgeJsonCols,data = [edge_row_json])], ignore_index=True)                      
                    #increment data
                    lastKey = key
                    lastPoint = (mid_lat,mid_lon)
                    lastCountry = country 
                    if month == 12:
                        year += 1
                        month = 1
                    else:
                        month += 1
                    date = pd.to_datetime(str(year)+'-'+str(month)+'-1')
#write data out to summary files
nodes.to_csv('D:/511_Project/Processed_Data/nodes.csv',index = False)
edges_json.to_csv('D:/511_Project/Processed_Data/edges.tsv',sep='\t',index = False)
edges_json.to_json('D:/511_Project/Processed_Data/json/merged_migration.json')




#select best examples by species
df = edges_json
selectionCols = ['species','flock','nbirds','duration','continuity_%']
selectionDF = pd.DataFrame(columns = selectionCols)
for s in list(df['species'].unique()):
    #selectionCols = ['species','id','nbirds','duration','continuity_%']
    #selectionDF = pd.DataFrame(columns = selectionCols)
    for i in list(df['flock'].unique()):
        points = df[(df['species']==s) & (df['flock']==i)].shape[0]
        if points > 1 :
            nbirds = df[(df['species']==s) & (df['flock']==i)]['nbirds'].max()
            minDate = df[(df['species']==s) & (df['flock']==i)]['date'].min()
            maxDate = df[(df['species']==s) & (df['flock']==i)]['date'].max()
            duration = (maxDate.year - minDate.year) * 12 + maxDate.month - minDate.month
            continuity = (points*100.0)/duration
            selectionRow = [s,i,nbirds,duration,continuity]
            selectionDF = pd.concat([selectionDF, pd.DataFrame(columns = selectionCols,data = [selectionRow])], ignore_index=True)
    selectionDF = selectionDF[(selectionDF['duration'] > 36) & (selectionDF['continuity_%'] > 10)].sort_values(by=['duration'],ascending = False)

#set best example to 1, second best to 2, etc.
top_rows = selectionDF.sort_values(by=['species','continuity_%'],ascending = False).groupby('species').head(5)
top_rows['exampleRank'] = top_rows.groupby('species').cumcount() + 1
#left join datasets to map the new example rank to the species flock
selected = pd.merge(edges_json,top_rows,on =['species','flock'],how='left')
#filter out all the unranked data
selected = selected[selected['exampleRank']>0]
#filter selected columns here and rename flock to exampleRank
del selected['nbirds_y'], selected['index']
selected.rename(columns={'nbirds_y':'nbirds','flock':'id','exampleRank':'flock'}, inplace=True)
selected = selected.reset_index()
selected.to_csv('D:/511_Project/Processed_Data/edges_final.tsv',sep='\t',index = False)
selected.to_json('D:/511_Project/Processed_Data/json/merged_migration_final.json')

#read in DFs
migrationCols = ['species','flock','year', 'season','observations','Dest_coords']
migrationDF = pd.DataFrame(columns = migrationCols)
for s in selected['species'].unique():
    dat = selected[selected['species'] == s]
    for f in dat['flock'].unique():
        flock_dat = dat[dat['flock'] == f]
        year = min(flock_dat['date']).year
        if min(flock_dat['date']).month >4 and min(flock_dat['date']).month <11:
            season = 'summer'
        else:
            season = 'winter'
        print(s+'-'+str(f))
        while year <= max(flock_dat['date']).year:
            if season == 'summer':
                season_dat = flock_dat[(flock_dat['date'] >= pd.to_datetime(str(year)+'-4-1')) & (flock_dat['date'] <= pd.to_datetime(str(year)+'-10-1'))]
            else:
                season_dat = flock_dat[(flock_dat['date'] >= pd.to_datetime(str(year-1)+'-10-1')) & (flock_dat['date'] <= pd.to_datetime(str(year)+'-4-1'))]
            observations = season_dat.shape[0]
            #avg bird migrations [3->6 & 8->11]
            if observations > 2:
                #get northern most point
                #try to filter to points with least distance traveled
                if season_dat[season_dat['distanceCount']<400].shape[0]>1:
                    points =[]
                    for p in season_dat[season_dat['distanceCount']<400]['coords']:
                        if len(points) == 0:
                            points.append((p[0][1],p[0][0],1))
                        points.append((p[1][1],p[1][0],1))
                    #get the goedesic average of those points
                    mid_lat, mid_lon =  hf.geographic_midpoint(points)
                    dest_point = [mid_lat,mid_lon]
                    point_type = 'average'
                #otherwise get the northern or southern most point seen in this timeframe
                else:
                    count = 0
                    if season == 'summer':
                        for p in season_dat['coords']:
                            if p[0][1] > p[1][1]:
                                if dest_point[0] < p[0][1] or count == 0:
                                    dest_point = [p[0][1],p[0][0]]
                                    count+=1
                            else:
                                if dest_point[0] < p[1][1]:
                                    dest_point = [p[1][1],p[1][0]]
                                    count+=1
                    else:
                        for p in season_dat['coords']:
                            if p[0][1] < p[1][1]:
                                if dest_point[0] > p[0][1] or count == 0:
                                    dest_point = [p[0][1],p[0][0]]
                                    count+=1
                            else:
                                if dest_point[0] > p[1][1]or count == 0:
                                    dest_point = [p[1][1],p[1][0]]
                                    count+=1
                    point_type = 'individual'
                print('found migration dat for ' + season + '-' + str(year) + ' as an ' + point_type)                    
                migrationCols = ['species','flock','year', 'season','observations','Dest_coords']
                migrationRow = [s,f,year,season,observations,dest_point]
                migrationDF = pd.concat([migrationDF, pd.DataFrame(columns = migrationCols,data = [migrationRow])], ignore_index=True)
            if season == 'summer':
                year +=1
                season = 'winter'
            else:
                season = 'summer'


for s in migrationDF['species'].unique():
    for f in migrationDF['flock'].unique():
        df = migrationDF[(migrationDF['species']== s) & ( migrationDF['flock'])==f]
        distances = []
        for c in range(1,df.shape[0]-1):
            #print(str(df.iloc[c-1][2])+'-'+str(df.iloc[c][2])+'-'+str(df.iloc[c-1][3])+'-'+str(df.iloc[c][3]))
            if hf.is_consecutive(df.iloc[c-1][2],df.iloc[c][2],df.iloc[c-1][3],df.iloc[c][3]):
                tot_dist,ns_dist,ew_dist = hf.distance_components(df.iloc[c-1][5][0], df.iloc[c-1][5][1], df.iloc[c][5][0], df.iloc[c][5][1])
                distances.append(tot_dist)
        if len(distances) >0:
            avg_dist = sum(distances)/len(distances)
            print(s + '-' + str(f) + ' traveled an average distance of ' + str(round(avg_dist,2)) + 'kms during its migrations')

                
              
