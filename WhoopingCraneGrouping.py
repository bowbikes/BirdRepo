#To be 100% transparent, this script should probably be broken into 3-4 separates scripts
#It was just easier for me to have it all in once place as I was working on it.
import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import helperFunctions as hf
from dateutil.relativedelta import relativedelta

#getting data into pandas
#this is the output from the final sql query that pulls from our azure database of all the bird data
df = pd.read_csv('D:/511_Project/FinalQueryResults.csv')
#df = pd.read_csv('D:/511_Project/Goose_and_eagle.csv') # this is a special subset of hand selected data
#just checking to make sure we dont have any duplicate data
df = df.drop_duplicates()
#converting datatypes
df['EVENT_DATE']= pd.to_datetime(df['EVENT_DATE'],format='%m/%d/%Y')
#thresholds used to define the flocks (if a bird is seen within a few kilometers of another bird within a few days of each other then we can say theyre friends)
time_threshold = 2 #days
dist_threshold = 5 #kilometers

#tag flocks for each bird species
#2040 = Whooping Crane; 3490 = Golden Eagle; 1940 = Blue Heron; 1720 = canada goose
for s in [2040,3490,1940,1720]:
    #specifiy which species we are looking at
    #filter all the query data to just the species we are looking at
    dat = df[df['SPECIES_ID']==s]
    dat = dat.sort_values(by=['EVENT_DATE'])
    #create an empty list to store the links between birds that are found later
    friends = []
    print('starting on species code ' + str(s))
    #for each bird
    for b in list(dat['ORIGINAL_BAND'].unique()):
        #get every time we see this individual bird
        bird_dat = dat[dat['ORIGINAL_BAND']==b]
        #take a look at each time we see this individual bird
        for e in list(bird_dat['EVENT_DATE'].unique()):
            #expand our search window aroudn the particular event we are looking at
            #get other events around that time (plus or minus the time threshold)
            event_dat = dat[(dat['EVENT_DATE'] > (e - pd.Timedelta(days=time_threshold))) & (dat['EVENT_DATE'] <= (e + pd.Timedelta(days=time_threshold)))]
            #extract the original coordinates for this individual bird and this specific sighting event
            orig_coords = (event_dat[(event_dat['ORIGINAL_BAND']==b)&(event_dat['EVENT_DATE']==e)].iloc[0,3],event_dat[(event_dat['ORIGINAL_BAND']==b)&(event_dat['EVENT_DATE']==e)].iloc[0,4])
            #ignore the current event in question because we dont want to double count it
            event_dat = event_dat[event_dat['ORIGINAL_BAND']!=b]
            #see if any of the remaining events are close in distance to the coordinates for this specific event 
            for i in range(0,event_dat.shape[0]-1):
                #extract the coordinates for any sighting event that were within the time window
                test_coords = (event_dat.iloc[i,3],event_dat.iloc[i,4])
                #create link between two birds if the coordinates are within the distance threshold
                if hf.areSameFlock(orig_coords,test_coords,dist_threshold):
                    friends.append([b,event_dat.iloc[i,1]])
    print('finished tagging each bird of this species')
    #look at link lists to try to determind flocks
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
del df, dat, flock_df,friends,groups

# read in EIA generation Data this was processed using their API in the eiaData.py file
EIA_df = pd.read_csv('D:/511_Project/Formated_GeneratorData.csv')
# filter to generator types that would have a plausible effect on birds
EIA_df = EIA_df[EIA_df['Category'].isin(['Hydro','Coal','Petroleum','Wind'])]
#converting datatypes
EIA_df['date']= pd.to_datetime(EIA_df['date'],format='%Y-%m')
#variable and DataFrame initialization
nodeId = 0
nodeCols = ['record_id','species','flock','nbirds','latitude','longitude','date','isoCountry','band']
nodes = pd.DataFrame(columns = nodeCols)
edgeJsonCols = ['species','flock','date','coords','fossilCount','windCount','borderCount','hydroCount','distanceCount','nbirds']
edges_json = pd.DataFrame(columns = edgeJsonCols)
#this loop condensces all geospacial data into a pretty line for graphing
for s in ['Whooping_Crane','Golden_Eagle','Blue_Heron']:
    #Read in preprocessed bird sighting data
    fp = 'D:/511_Project/Processed_Data/'+s+'/flockTest.csv'
    df = pd.read_csv(fp)
    #convert to datetime for utility
    df['EVENT_DATE']= pd.to_datetime(df['EVENT_DATE'],format='%Y-%m-%d')
    # start looking at flocks instead of individual birds
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
                #intiialize counters to track the features we are interested in
                FossilCounter = 0
                WindCounter = 0
                HydroCounter = 0
                BorderCounter = 0
                #filter the bird dataframe to only sighting events within the year and month the loop is looking at
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
                node_row = [key,s,f,month_dat.shape[0],mid_lat,mid_lon,date,country,list(df['flock'])[0]]
                #append rows to data_frames
                nodes = pd.concat([nodes, pd.DataFrame(columns = nodeCols,data = [node_row])], ignore_index=True)
                #check if this is the first point in the loop, if it isnt then generate route segment data
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
                    #filter generators using the GPS rectangle boundary mask
                    df_rect = EIA_df[rect_mask].copy()
                    #filter out generator not built yet
                    df_rect = df_rect[df_rect['date']<=date]
                    #for all the remaining generators check their position relative to the flight path
                    for g in range(0,df_rect.shape[0]-1):
                        gen_lat = df_rect.iloc[g][0]
                        gen_lon = df_rect.iloc[g][1]
                        #this function sees if any are within 10km of the start position, end position, or along the path between and increments the approriate counter based on the type
                        #wind farms need more area for the same nameplate capacity so their threshold is bumped up to 20km
                        if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<10.1 and (df_rect.iloc[g][6] == 'Coal' or df_rect.iloc[g][6] == 'Petroleum')):
                            FossilCounter += 1
                        if(hf.min_distance_to_path(lastPoint[0],lastPoint[1],mid_lat,mid_lon,gen_lat,gen_lon)<20.1 and (df_rect.iloc[g][6] == 'Wind')):
                            WindCounter += 1
                    if lastCountry!=country or month_dat['ISO_COUNTRY'].nunique()>1:
                        BorderCounter += 1
                    #log edge data: species, flock, source, target, distance, ew_dist,ns_dist, heading, fossil fuel counter, wind farm counter, border_crossing counter
                    edge_row_json = [s,f,date - pd.DateOffset(months=1),[[lastPoint[1],lastPoint[0]],[mid_lon,mid_lat]],FossilCounter,WindCounter,BorderCounter,HydroCounter,total_dist,nbirds]
                    #append rows to data_frames
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
        #this runs the same code but for individual birs that do not have the same flock so I won't be commenting it again, all the logic is identical
        #it could probably be turned into a function for aesthetics eventually
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
                    node_row = [key,s,f,month_dat.shape[0],mid_lat,mid_lon,date,country,b]
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
#tsv for debugging and json for use with the final visual
edges_json.to_csv('D:/511_Project/Processed_Data/edges.tsv',sep='\t',index = False)
edges_json.to_json('D:/511_Project/Processed_Data/json/merged_migration.json')

#select best examples by species
#this could probably be its own script too
#df = pd.read_csv('D:/511_Project/Processed_Data/edges.tsv',sep='\t')
df = edges_json# could read inthe data that was written out from the last section
#create a dataframe to organize the the birds that will show up best on our chart.
selectionCols = ['species','flock','nbirds','duration','continuity_%']
selectionDF = pd.DataFrame(columns = selectionCols)
for s in list(df['species'].unique()):
    for i in list(df['flock'].unique()):
        #filter data to an individual species, flock pair
        points = df[(df['species']==s) & (df['flock']==i)].shape[0]
        #make sure we have more than one route segment
        if points > 1 :
            #these are all features I was exploring to determine what would best visualize the birds.
            nbirds = df[(df['species']==s) & (df['flock']==i)]['nbirds'].max()
            minDate = df[(df['species']==s) & (df['flock']==i)]['date'].min()
            maxDate = df[(df['species']==s) & (df['flock']==i)]['date'].max()
            duration = (maxDate.year - minDate.year) * 12 + maxDate.month - minDate.month
            continuity = (points*100.0)/duration
            selectionRow = [s,i,nbirds,duration,continuity]
            selectionDF = pd.concat([selectionDF, pd.DataFrame(columns = selectionCols,data = [selectionRow])], ignore_index=True)
    #Ultimately I decided that the best data was data that was on the screen for the longest and was moving for the largest percentage of that time
    selectionDF = selectionDF[(selectionDF['duration'] > 36) & (selectionDF['continuity_%'] > 10)].sort_values(by=['duration'],ascending = False)
#set best example to 1, second best to 2, etc.
top_rows = selectionDF.sort_values(by=['species','continuity_%'],ascending = False).groupby('species').head(5)
top_rows['exampleRank'] = top_rows.groupby('species').cumcount() + 2
#left join datasets to map the new example rank to the species flock
selected = pd.merge(edges_json,top_rows,on =['species','flock'],how='left')
#filter out all the unranked data
selected = selected[selected['exampleRank']>0]
#filter selected columns here and rename flock to exampleRank
del selected['nbirds_y'], selected['index']
selected.rename(columns={'nbirds_y':'nbirds','flock':'id','exampleRank':'flock'}, inplace=True)
#format the data and write it out for the last time, this is the final dataframe we ended up visualizing
selected = selected.reset_index()
selected.to_csv('D:/511_Project/Processed_Data/edges_final.tsv',sep='\t',index = False)
selected.to_json('D:/511_Project/Processed_Data/json/merged_migration_final.json')

#this code never ended up giving us what we wanted to see
#it was intended to summarize a species-flock's migrations per year
#I think the underlying data inconsistency doomed it
#this could be its own script as well
#read in DFs
#selected = pd.read_csv('D:/511_Project/Processed_Data/edges_final.tsv',sep='\t')
#initialize summary dataframes
migrationCols = ['species','flock','year', 'season','observations','Dest_coords']
migrationDF = pd.DataFrame(columns = migrationCols)
#this first loop identifies migration destinations by season
for s in list(selected['species'].unique()):
    dat = selected[selected['species'] == s]
    for f in list(dat['flock'].unique()):
        #filter data to an individual species, flock pair
        flock_dat = dat[dat['flock'] == f]
        #get the first year and season that this bird/flock was seen
        year = min(flock_dat['date']).year
        if min(flock_dat['date']).month >4 and min(flock_dat['date']).month <11:
            season = 'summer'
        else:
            season = 'winter'
        #we want to look season by season year after year and check if there is enough data
        #to generate any summary statistics about that year and that seasons's migration
        while year <= max(flock_dat['date']).year:
            #if its summer we want to filter to the relevant summer months, otherwise weant to 
            #filter to the relevant witnther months with some buffer in both cases
            if season == 'summer':
                season_dat = flock_dat[(flock_dat['date'] >= pd.to_datetime(str(year)+'-4-1')) & (flock_dat['date'] <= pd.to_datetime(str(year)+'-10-1'))]
            else:
                season_dat = flock_dat[(flock_dat['date'] >= pd.to_datetime(str(year-1)+'-10-1')) & (flock_dat['date'] <= pd.to_datetime(str(year)+'-4-1'))]
            observations = season_dat.shape[0]
            #avg bird migration months [3->6 & 8->11]
            # we only give migration summary stats if there's more than 2 routes seen within the 7 month span
            if observations > 2:
                #if we have lots of points within the date range than the migration destination is where the bird is moving the least
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
                #otherwise get the northern or southern most point seen in this timeframe depending on the season we are looking for
                else:
                    #this could be made to look a lot better by making it a function
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
                #if there was data generated add it to the datastructure and then keep looping
                print('found migration dat for ' + season + '-' + str(year) + ' as an ' + point_type)                    
                migrationRow = [s,f,year,season,observations,dest_point]
                migrationDF = pd.concat([migrationDF, pd.DataFrame(columns = migrationCols,data = [migrationRow])], ignore_index=True)
            #increment the loop by jumping to the next consecutive season
            if season == 'summer':
                year +=1
                season = 'winter'
            else:
                season = 'summer'

#after getting the migration destination we want to scrub though that list to give context 
#to the actual flight routes to and from the destinations identified above
for s in migrationDF['species'].unique():
    for f in migrationDF['flock'].unique():
        df = migrationDF[(migrationDF['species']== s) & ( migrationDF['flock'])==f]
        distances = []
        for c in range(1,df.shape[0]-1):
            #we only want to generate statistics about a migration when we know they happend right after each other
            if hf.is_consecutive(df.iloc[c-1][2],df.iloc[c][2],df.iloc[c-1][3],df.iloc[c][3]):
                tot_dist,ns_dist,ew_dist = hf.distance_components(df.iloc[c-1][5][0], df.iloc[c-1][5][1], df.iloc[c][5][0], df.iloc[c][5][1])
                distances.append(tot_dist)
        if len(distances) >0:
            avg_dist = sum(distances)/len(distances)
            print(s + '-' + str(f) + ' traveled an average distance of ' + str(round(avg_dist,2)) + 'kms during its migrations')

                
              
