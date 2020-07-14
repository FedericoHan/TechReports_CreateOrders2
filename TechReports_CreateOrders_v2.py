# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 12:30:46 2020

@author: Fed

Goal1: create big snapshot with all the levels in one page. First created a collection of dataframes all at once (object: dfWithTechLevels) and
then created the snapshots (object dfLatestTechs, Latest is baiscally todays.)
Goal2: automate creation of orders (object: CreateOrders)

Version2: shortened comments, like "Gamma Odas ..StDevs" to "Lah ..SD".  Added name to generated export files, so can create some for same currency but diff books..ie USDCNHSG1.xls and USDCNHSG2.xls
"""

import DownloadData_v2
import datetime as dt#  --->NameError: name 'datetime' is not defined

#from datetime import date
import numpy as np
import pandas as pd

#from jinja2 import Environment, FileSystemLoader

class dfWithTechLevels(object):
    
    '''takes as input df of daily or hourly prices with MultiIndex Columns (SomeCurncy, PX_LAST)
    then returns df with extra bunch of tech indicators like MAs,realized_vol_annualized, and highest & lowest rolling [55days] closes'''
    
    def __init__(self, df_input, obs_tenor = 'D', vol_tenor = 21):
        
        self.df_input = df_input 
        self.obs_tenor = obs_tenor
        self.vol_tenor = vol_tenor
        
        if self.obs_tenor == 'D':
            self.df_techs = self.create_techs(vol_tenor = 21)
        elif self.obs_tenor == 'H':
            self.df_techs = self.create_techs_hourlies()
        
    
    def create_techs(self, vol_tenor = 21):
        
        df_techs = self.df_input
        
        for col in self.df_input: #col will return tuple (SomeCurncy, PX_LAST)
            if 'PX_LAST' in col: #perform operations only on  PX_LAST.  col[0] returns first elment of tuple, ie SomeCurncy
                df_techs[col[0]+'_21DMA'] = self.df_input[col].rolling(21, min_periods = 1).mean()#maybe try self.df_input.loc[:,col].rolling(21, min periods =1).mean()
                df_techs[col[0]+'_55DMA'] = self.df_input[col].rolling(55, min_periods = 1).mean()
                df_techs[col[0]+'_100DMA'] = self.df_input[col].rolling(100, min_periods = 1).mean()
                df_techs[col[0]+'_200DMA'] = self.df_input[col].rolling(200, min_periods = 1).mean()#rolling(200, center = TRUE-->WRONG average)
                df_techs[col[0]+'_55_periods_high'] = self.df_input[col].rolling(55, min_periods = 55).max() #currently using PX_LAST, later jsut feed PX_HIGH
                df_techs[col[0]+'_55_periods_low'] = self.df_input[col].rolling(55, min_periods= 55).min() #currenlty_using PX_LAST, late just feed PX_LOW
                
                df_techs[col[0]+'_realized_vol_unit'] = self.df_input[col].pct_change().rolling(vol_tenor, min_periods = vol_tenor).std()#this is daily or weekly
                if self.obs_tenor == 'D':
                    df_techs[col[0]+'_realized_vol_annual'] = self.df_input[col].pct_change().rolling(vol_tenor, min_periods = vol_tenor).std()*np.sqrt(255/1)#annualize days
                elif self.obs_tenor == 'W':
                    df_techs[col[0]+'_realized_vol_annual'] = self.df_input[col].pct_change().rolling(vol_tenor, min_periods = vol_tenor).std()*np.sqrt(52)#annaulize weeks
                
        return df_techs 
    
    def create_techs_hourlies(self):
        
        df_techs_hourlies = self.df_input
        
        for col in df_techs_hourlies:
            df_techs_hourlies[col[0]+'_55HMA'] = df_techs_hourlies[col].rolling(55, min_periods = 1).mean()
            df_techs_hourlies[col[0]+'_200HMA'] = df_techs_hourlies[col].rolling(200, min_periods = 1).mean()
        return df_techs_hourlies
    

class dfLatestTechs(dfWithTechLevels):
    '''takes latest values from dfWithTechLevels (dailies or hourlies) and formats them, plus one method to merge dailies & hourly'''
    
    def __init__(self, df_input, obs_tenor):
        super().__init__(df_input, obs_tenor)   
        
        #added later for merge method
        print(obs_tenor)
        if obs_tenor == 'D':
            self.df_signals = self.format_signals()
        elif obs_tenor == 'H':
            self.df_hourly = self.format_hourlies()

    def format_signals(self, obs = 255):
        
        ##create df for latest daily signals 
        df_signals = self.df_techs.tail(1).transpose()
        #create index 0 to n 
        df_signals = df_signals.reset_index()
        #rename columns ..'LAST' just useful for procedure below
        df_signals.columns = ['Instru', 'LAST', 'VALUE']
        #sorts rows such as IHN+1M Curncy, IHN_1M_Curncy_100dma etc
        df_signals.sort_values('Instru', inplace = True)
        
        df_signals['flag'] = df_signals['LAST'] == 'PX_LAST'#temporary boolean flag TRUE for anchor (spot & 1s have PX_LAST under 'LAST' column)
        df_signals['Dumbo'] = df_signals['VALUE'].where(df_signals['flag']).ffill()#temp col, if flagabove = True, ffill value to create temp columns to subtract values from MA
        df_signals['Distance_%'] = round((df_signals['VALUE'] / df_signals['Dumbo']) - 1.0,4)#distance from moving average in standard deviations
        
        df_signals['flag2'] = df_signals['Instru'].str.contains('vol_unit')#temporary boolean flag = "TRUE' if Instru col containts vol_unit
        df_signals['Dumbo2'] = df_signals['VALUE'].where(df_signals['flag2']).bfill()#temorary col  where if flag2 is true bfill so then can subtract
        df_signals['Distance_vol'] = round((df_signals['VALUE'] - df_signals['Dumbo']) / (df_signals['VALUE'] * df_signals['Dumbo2']),2)# (Price - MA)/(price* vol)
                                                                    
        df_signals['Trend'] = np.where( (df_signals['Distance_%'].values < 0.0), "Up", "Down")
        df_signals['Trend'] = np.where(df_signals['LAST'].isnull(), 0, df_signals['Trend'])
        
        df_signals = df_signals.drop( ['LAST', 'flag', 'Dumbo', 'flag2', 'Dumbo2'], axis = 1)#if forget how above works dont remove Dumbo so you can see what you did
        return df_signals
    
    def format_hourlies(self):
        '''feed df of hourly prices & techs '''
        #create df for latest hourly signals..transposforming places has currnecies as index
        df_hourly = self.df_techs.tail(1).transpose()
        #create index 0 to n
        df_hourly = df_hourly.reset_index()
        df_hourly.columns = ['Instru', 'Type', 'VALUE']#Type is kinda useless so drop it
        df_hourly = df_hourly.drop(['Type'] , axis = 1)
        
        return df_hourly
    
    def merge_hourly_and_daily(self):
        #merge daily and hourlies
        df_signals_all = self.df_signals.append(self.df_hourly).drop_duplicates(subset = 'Instru')#if you have USDCNH Curcny on both df['Instru'] get rid of one
        return df_signals_all
    
     #   cnh = CreateOrders(merged_asia, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG1', Ccy1 = 'USD', Ccy2 = 'CNH', \
      #                FixedCcy = 'USD', Tenor = 'SPT', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
       #               amount_basic = 1e6, odas_below = 2, odas_above = 3, gamma_local = 1, gamma_below =  1, gamma_above = -1) 

    #cnh_format = cnh.create_orders_df()
   # cnh_export = cnh.format_create_orders_df('xls') 

class CreateOrders(object):
    '''Takes as input MergedAsia,then create_orders_df will filter out currency of interest, and return a df 
    with odas by some logic, in format matching what Barraquda requires and as i type this i wonder if this can be moved to 
    format_create_orders which returns an xls or csv for easy upload into Barra'''
    #below Class variables for uncommon columns, still needed to upload into Barra , usually '' for all instances
    ID = '' 
    Parent = '' 
    Peer = ''
    Relationship = '' 
    Side = ''  
    ValueDate = '' 
    Product = ''
    FixingDate = ''
    Tracking = '' 
    RequestingUser = ''
    Markup = ''
    TrailingPips = ''
    TrailingTrigger = ''
    AllowPartial = ''
    SLSlippage = ''
    HoldSTP = ''
    BMBulkEligible = ''
    #the actual uploadable barraquada file nees an empty column, more deets below  
    SettlementType = 'CLS'#CLS, CSH or GRS
    
    
    def __init__(self, df_tech_levels, distance_from_techs, order_Type,\
                     Client, Account, Ccy1, Ccy2, FixedCcy,  Tenor, Activation, Expiry, Fixing, Comment_client, Comment_private,\
                     amount_basic, odas_below = 3, odas_above = 3, gamma_local = 1, gamma_below = 1, gamma_above = 1):
        
        self.df_tech_levels = df_tech_levels#df with latest tech levels..so ideally df_signals from dfLatestTechs Class
        self.distance_from_techs = distance_from_techs#0.1; scaling factor*daily_vol, eg 50% * 0.0025, the smaller the neaarer to the tech levels
        
        self.order_Type = order_Type #TP....Type may be reserved word so changed to order_Type
      
        self.Client = Client#CP85VC 
        self.Account = Account##FXOETFSG1 
        
        self.Ccy1 = Ccy1#USD
        self.Ccy2 = Ccy2#TWD
        self.pair = str(self.Ccy1) + str(self.Ccy2)#USDTWD
        
        #self.Intent = Intent #B / S flag later
        #self.Amount = Amount #This one we calcualte with create_orders df
        self.FixedCcy = FixedCcy
        
        self.Tenor = Tenor#SPT or 1M
        #self.Rate = Rate #this one we calcluate with create_orders_df()
        self.Activation = Activation#NOW
        self.Expiry = Expiry#tomorrow at some time
        self.Fixing = Fixing
        self.Comment_client = Comment_client
        self.Comment_private = Comment_private
        
        self.amount_basic, self.odas_below, self.odas_above = amount_basic, odas_below, odas_above
        #can use these as multipliers to increase / decrease notionals based on gamma
        self.gamma_local, self.gamma_below, self.gamma_above = gamma_local, gamma_below, gamma_above
        
        

    def create_orders_df(self):

        '''begins creating df with orders etc.  number odas, gamma either 1.0 for long or -1.0 for short
        for now gamma ignore...use odas_below and odas above to control'''

        pair = self.pair#USDTWD,EURUSD,etc
        
        #reformat to match the NDF convention
        if pair == 'USDIDR':
            pair = 'IHN' #should be assignment, not boolean?
        elif pair == 'USDKRW':
            pair = 'KWN'
        elif pair == 'USDTWD':
            pair = 'NTN'#for somre reason str.contains NTN+1M doesnt find it
        elif pair == 'USDPHP':
            pair = 'PPN'
  
        #filter out from merged_df the currency of interest, eg USDTWD --> NTN+1M       
        df_pair = self.df_tech_levels[self.df_tech_levels.loc[:, 'Instru'].str.contains(pair)]
        #since wont use hourly odas for now delete 55hma and 200hma or calcs below break down
        df_pair = df_pair[~df_pair.Instru.str.contains('HMA')]
        
        #creates two extra columns, to make performing df.at operations more easily
        #based on:  https://stackoverflow.com/questions/61926275/pandas-row-operations-on-a-column-given-one-reference-value-on-a-different-col
        df_pair[['Fruit', 'Descr']] = df_pair['Instru'].str.split('_', n = 1, expand = True) #df_pair['Instru']looks like 'EURCNH Curncy_55DMA' etc
        #will use this as Index to use the df.at method later
        df_pair = df_pair.set_index('Descr')
        df_pair.index = df_pair.index.fillna('empty') #rename 'na' or ' ', ie, EURCNH Curncy with 'empty' for df.at below
        
        gamma_range_local = (df_pair.at['empty', 'VALUE']*(1-df_pair.at['realized_vol_unit', 'VALUE']), df_pair.at['empty','VALUE']*(1+df_pair.at['realized_vol_unit','VALUE']))
        print('gamma_range_local '+str(gamma_range_local))
        
        ########
        #begin creating dataframe that will become Barra report.  First create columns to match Barraquda inputs, generally blank
        ########
        
        df_pair['ID'] = CreateOrders.ID#blank class var
        df_pair['Parent'] = CreateOrders.Parent#blank class var
        df_pair['Peer'] = CreateOrders.Peer #blank class var
        df_pair['Relationship'] = CreateOrders.Relationship#blank class var
        
        #########
        #These columns below are actually important, class attributes defined by user at instantation 
        #########
        
        df_pair['Type'] = self.order_Type
        df_pair['Side'] = self.Side
        df_pair['Client'] =self.Client
     
        df_pair['Account'] = self.Account
        df_pair['Ccy1'] = self.Ccy1
        df_pair['Ccy2'] = self.Ccy2
        #########
        #CORE of work is here..first  create buy_sell_flag called 'intent'
        df_pair['Intent'] = np.where(df_pair['Trend'] == 'Down', 'S', 'B')
        df_pair['Amount'] = self.amount_basic#init
        
        df_pair['Fixed Ccy'] = self.Ccy1 #useless
        df_pair['Value Date'] = CreateOrders.ValueDate #blank class var
        df_pair['Tenor'] = self.Tenor 
               
        df_pair['Rate'] = 0.0 #init
        
        #####
        # below is for odas near techincal levels
        #e.g. TP to sell, pick resistance - (daily_vol * margin, else pick support + (daily_vol*margin))
        ######
        df_pair['Rate'] = (df_pair['VALUE']*(1- (df_pair.at['realized_vol_unit','VALUE'])*self.distance_from_techs)).where(df_pair['Intent']=='S',\
                                                                                                                           df_pair['VALUE']*(1 + (df_pair.at['realized_vol_unit','VALUE'] * self.distance_from_techs)))
 
        #then create odas based on stdev
        num_odas = self.odas_above + self.odas_below
        ##create num_odas new rows
        df_pair_annex = pd.DataFrame([df_pair.loc['realized_vol_unit']] * num_odas)
        #then  create odas tech levels
        #print('odas_ above :'+str(self.odas_above))
        counter_above = 1
        counter_below = 1
        
        for i in range(num_odas):
            #add odas above
            if i < self.odas_above:
                
                df_pair_annex['Rate'].iloc[i] = df_pair.at['empty', 'VALUE'] * (1 + df_pair.at['realized_vol_unit', 'VALUE']*(counter_above))#spot or NDF * (1 + stDev) * counter
                df_pair_annex['Instru'].iloc[i] = 'Gamma oda '+str(counter_above)+str(' stDev above')
                
                #multipy notional by gamma_local if in local zone or by gamma_above if above
                if df_pair_annex['Rate'].iloc[i] < gamma_range_local[1]: #gamma_range_local = tuple(lower_bound, higher_bound) [0, 1]
                    df_pair_annex['Amount'].iloc[i] = df_pair_annex['Amount'].iloc[i] * self.gamma_local
                df_pair_annex['Amount'].iloc[i] = df_pair_annex['Amount'].iloc[i] * self.gamma_above
                
                counter_above += 1

                if df_pair_annex['Rate'].iloc[i] >= df_pair.at['empty', 'VALUE']:#because df_pair_annex just takes the Intent from .loc['realized_vol_unit], need to update, so if Oda > Spot, = Sell if TP
                    df_pair_annex['Intent'].iloc[i] = 'S'#== doesnt work as boolean test?
                else:
                    df_pair_annex['Intent'].iloc[i] = 'B'

            #add odas below          
            else:
                df_pair_annex['Rate'].iloc[i] = df_pair.at['empty', 'VALUE'] * (1 - df_pair.at['realized_vol_unit', 'VALUE']*(counter_below))
                df_pair_annex['Instru'].iloc[i] = 'Gamma oda '+str(counter_below)+str(' stDev below')
                
                #multipy notional by gamma_local if in local zone or by gamma_above if above
                if df_pair_annex['Rate'].iloc[i] > gamma_range_local[0]: #gamma_range_local = tuple(lower_bound, higher_bound) [0, 1]
                    df_pair_annex['Amount'].iloc[i] = df_pair_annex['Amount'].iloc[i] * self.gamma_local
                df_pair_annex['Amount'].iloc[i] = df_pair_annex['Amount'].iloc[i] * abs(self.gamma_below)
                
                counter_below += 1

                if df_pair_annex['Rate'].iloc[i] <= df_pair.at['empty', 'VALUE']:#because df_pair_annex just takes the Intent from .loc['realized_vol_unit], need to update, so if Oda < Spot, = Buy if TP
                    df_pair_annex['Intent'].iloc[i] = 'B'
                else:
                    df_pair_annex['Intent'].iloc[i] = 'S'
                 
        #merge df of odas near tech with df odas various st_devs away
        df_pair = df_pair.append(df_pair_annex)
        df_pair = df_pair.reset_index(drop = True).drop(columns = 'Fruit')

        #get rid of row that has pivot spot or 1s NDF, ie VALUE - Distance from Value == 0
        df_pair = df_pair[df_pair['Distance_%'] != 0.0]

        '''If want to scale amount by vol_distance
        create temp_columsn to see how far each row is from the next in vol units to scale notionals #'''

        df_pair = df_pair.sort_values(by = 'Rate', ascending = False)
        df_pair['Temp_col2'] = df_pair['Rate'].pct_change(periods = -1)#diff with folloiwng row, https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.diff.html?highlight=diff#pandas.DataFrame.diff
        #drop odas that are too close to each other
        df_pair = df_pair.drop(df_pair[df_pair.Temp_col2 < 0.001].index)
        
        ''' this code would have halved the notionals of clustered odas instead of dropping them 
        for i in range(len(df_pair)-1):
            #calculate distance between two odas, then scale order notionals by it or delete one of the two, print warning
            if df_pair['Temp_col2'].iloc[i] <= 0.001: #if distance between two odas is small
                df_pair['Amount'].iloc[i] = df_pair['Amount'].iloc[i] * 0.5 #
                df_pair['Amount'].iloc[i+1] = self.amount_basic * 0.5               
            else:
                df_pair['Amount'].iloc[i] = df_pair['Amount'].iloc[i]'''
       
 
        #below is for rigid odas                                                                                           
        df_pair['Activation'] = self.Activation

        #Please enter column R (expiry) as 19/05/2020 08:15 SGT
        expiry = dt.datetime.today() + pd.tseries.offsets.BusinessDay(n = 1) #expire 1Bd after today
        timestamp = expiry.strftime("%d/%m/%Y") + str(' 08:15 SGT')
        print(timestamp)

        df_pair['Expiry'] = timestamp#self.Expiry
     
        df_pair['Fixing'] = self.Fixing
        df_pair['Comment (Client)'] = ''       
        df_pair['Comment (Private)'] = df_pair['Instru']# what level corresponds too
        #shortens Comment Private to make more compact...
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('Gamma oda ', 'Lah ')#careful space after Lah
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('stDev ', 'SD')
        
        
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('21DMA', '21')
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('55DMA', '55')    
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('100DMA', '100')
        df_pair['Comment (Private)'] = df_pair['Comment (Private)'].str.replace('200DMA', '200')
        
        #####
        
        if self.Tenor == '1M': #NDFs need to have this field populated
            df_pair['Product'] = 'NDF'
        else:
            df_pair['Product'] = CreateOrders.Product#blank class var

        #create columns to match Barraquda inputs, generally blank

        df_pair['Fixing Date'] = CreateOrders.FixingDate#blank class var
        df_pair['Tracking'] = CreateOrders.Tracking#blank k var
        df_pair['Requesting User'] = CreateOrders.RequestingUser
        df_pair['Markup'] = CreateOrders.Markup
        df_pair['Trailing Pips'] = CreateOrders.TrailingPips
        df_pair['Trailing Trigger'] = CreateOrders.TrailingTrigger
        df_pair['Allow Partial'] = CreateOrders.AllowPartial
        df_pair['SL Slippage'] = CreateOrders.SLSlippage
        df_pair['Hold STP'] = CreateOrders.HoldSTP
        df_pair['BM BulkEligible'] = CreateOrders.BMBulkEligible
        
        #below needed as: "Also please note that in order for the spreadsheet to import with the settlementtype a blank column is required to the left of the settlementtype column."
        df_pair[''] = '' 
        
        df_pair['SettlementType'] = 'GRS'#ype #Gross Cash or CLS
        
        return df_pair 
    
    #changed xlsx to xls
    def format_create_orders_df(self, file_name,  csv_or_xls_flag):

        '''clean up the create_orders_df removing columns and rows not needed and then create .csv or .xlsx to upload into barraquada'''
        #call df from above method
        df_format = self.create_orders_df()
        #remove columns not in the template provided by Barracuda
        df_format = df_format.dropna(axis = 0) #drop rows with nan , ie 200hma and 55hma
        #df_format = df_format.drop( ['Instru','VALUE','Distance_%','Distance_vol', 'Trend', 'Temp_col_realized_vol_unit', 'Temp_col2_vol_difference'], axis = 1)
        df_format = df_format.drop( ['Instru','VALUE','Distance_%','Distance_vol', 'Trend', 'Temp_col2'], axis = 1)
        df_format = df_format[~df_format['Comment (Private)'].str.contains('vol')]#drop rows with vols value
       #set ID column 0 to n and then set as index
        df_format['ID'] = [n for n in range(len(df_format))]
        df_format = df_format.set_index('ID')

        #so rates dont have gazillions digits which Barra cant read
        dictio_floats = {'CNH': '%.4f',#EURCNH
                         'IDR': '%.2f',
                         'JPY': '%.2f',
                         'KRW': '%.2f',
                         'PHP': '%.2f',
                         'USD': '%.4f',#EURUSD
                         'TWD': '%.2f'}

        dictio_key = df_format.Ccy2.iloc[0]
        #print(dictio_key, dictio_floats[dictio_key])

        if csv_or_xls_flag == 'csv':
            df_format.to_csv(str(self.pair)+str(file_name)+str('.csv'), float_format = dictio_floats[dictio_key])#so rates dont have gazillions digits which Barra cant read
      
        elif csv_or_xls_flag == 'xls':#changed from xlsx     
            df_format.to_excel(str(self.pair)+str(file_name)+str('.xls'), float_format = dictio_floats[dictio_key])#so rates dont have gazillions digits which Barra cant read     
        return df_format

 
class TrendingOrders(CreateOrders):

    def __init__(self, df_tech_levels, distance_from_techs, order_Type,\
                     Client,   Account, Ccy1, Ccy2, FixedCcy,  Tenor, Activation, Expiry, Fixing, Comment_client, Comment_private):
        super().init(self, df_tech_levels, distance_from_techs, order_Type,\
                     Client, Account, Ccy1, Ccy2, FixedCcy,  Tenor, Activation, Expiry, Fixing, Comment_client, Comment_private)

 
    def create_orders_trend(self):
        pass

if __name__ == "__main__":
    
    #set dates
    start = dt.datetime(2017, 1, 20)  #year, month, day
    end = dt.datetime.today()
    #set periodicity 
    frequency = 'DAILY'
    #set pairs
    asia_ccys = ['USDCNH Curncy', 'EURCNH Curncy', 'IHN+1M Curncy',  'KWN+1M Curncy', 'PPN+1M Curncy',\
                 'USDSGD Curncy', 'USDTHB Curncy', 'NTN+1M Curncy']  #'IRN+1M Curncy',
        
    g10_ccys = ['AUDUSD Curncy', 'EURAUD Curncy', 'EURGBP Curncy', 'EURUSD Curncy', 'GBPUSD Curncy', 'USDJPY Curncy']
    #instantiate daily objects
    
    asia_ccys_objs = []
    for count in range(len(asia_ccys)):#count is 0 to n
        asia_ccys_objs.append('')#initialize list
        asia_ccys_objs[count] = DownloadData_v2.DownloadData(pair = asia_ccys[count], fields = ['PX_LAST'], startDate = start,\
                                            endDate = end, period = frequency, source = 'blp')
    #populate historical dataframes for Asia pair
    
    df_asia_dailies = {}
    count = 0
    for pair in asia_ccys:
        df_asia_dailies[pair] = []#initialize dictionary
        df_asia_dailies[pair] = asia_ccys_objs[count].get_data_blp_historical()
        count += 1
    #print(df_asia_dailies)
    df_asia_dailies = pd.concat([df_asia_dailies[i] for i in asia_ccys], join = 'outer', axis = 1, keys =asia_ccys)
    #convert from object to floating so can get rid of NA and calc MAs
    df_asia_dailies = df_asia_dailies.apply(pd.to_numeric, errors = 'coerce', axis = 0)
    
    #create hourly container
    df_asia_hourlies = {}
    
    count = 0
    for pair in asia_ccys: 
        df_asia_hourlies[pair] = []
        df_asia_hourlies[pair] = asia_ccys_objs[count].get_data_blp_intraday(-200, 60)
        count += 1
    
    df_asia_hourlies = pd.concat([df_asia_hourlies[i] for i in asia_ccys], join = 'outer', axis = 1, keys =asia_ccys)
    #filter out useless columns such as 'high, low, numEvents'
    df_asia_hourlies = df_asia_hourlies.iloc[:, df_asia_hourlies.columns.get_level_values(1) == 'close']
    #convert from object to floating so can get rid of NA and calc MAs
    df_asia_hourlies = df_asia_hourlies.apply(pd.to_numeric, errors = 'coerce', axis = 0)
    
    #instantiate developed 
    
    g10_objs = []
    for count in range(len(g10_ccys)):
        g10_objs.append('')
        g10_objs[count] = DownloadData_v2.DownloadData(pair = g10_ccys[count], fields = ['PX_LAST'], startDate = start, \
                                                    endDate = end, period = frequency, source = 'blp')
    
    df_g10_dailies = {}
    
    count = 0
    for pair in g10_ccys:
        df_g10_dailies[pair] = []
        df_g10_dailies[pair] = g10_objs[count].get_data_blp_historical()
        count += 1
    
    df_g10_dailies = pd.concat([df_g10_dailies[i] for i in g10_ccys], join = 'outer', axis = 1, keys =g10_ccys)
    #convert from object to floating so can get rid of NA and calc MAs
    df_g10_dailies = df_g10_dailies.apply(pd.to_numeric, errors = 'coerce', axis = 0)
    
     #create hourly container
    df_g10_hourlies = {}
    
    count = 0
    for pair in g10_ccys:
        df_g10_hourlies[pair] = []
        df_g10_hourlies[pair] = g10_objs[count].get_data_blp_intraday(-200, 60)
        count += 1
    
    df_g10_hourlies = pd.concat([df_g10_hourlies[i] for i in g10_ccys], join = 'outer', axis = 1, keys =asia_ccys)
    #filter out useless columns such as 'high, low, numEvents'
    df_g10_hourlies = df_g10_hourlies.iloc[:, df_g10_hourlies.columns.get_level_values(1) == 'close']
    #convert from object to floating so can get rid of NA and calc MAs
    df_g10_hourlies = df_g10_hourlies.apply(pd.to_numeric, errors = 'coerce', axis = 0)


    ######
    #Get latest data 
    
    asia_daily_obj = dfLatestTechs(df_asia_dailies, 'D') #instance of the children class
    asia_daily_sign = asia_daily_obj.format_signals() #df with latest
     
    asia_hourly_obj = dfLatestTechs(df_asia_hourlies, 'H') #instance
    asia_hourly_sign = asia_hourly_obj.format_hourlies()
     
    merged_asia = asia_daily_sign.append(asia_hourly_sign).drop_duplicates(subset = 'Instru')
     
    g10_daily_obj = dfLatestTechs(df_g10_dailies, 'D')
    g10_daily_sign = g10_daily_obj.format_signals() #df with latest
     
    g10_hourly_obj = dfLatestTechs(df_g10_hourlies, 'H') #instance
    g10_hourly_sign = g10_hourly_obj.format_hourlies()
    #aggregated df for G10 levels
    merged_g10 = g10_daily_sign.append(g10_hourly_sign).drop_duplicates(subset = 'Instru')
     
 

    ###########
    ##create odas
    ###########
    eur = CreateOrders(merged_g10, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG2', Ccy1 = 'EUR', Ccy2 = 'USD', \
                      FixedCcy = 'EUR', Tenor = 'SPT', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
                      amount_basic = 1e6, odas_below = 2, odas_above = 3, gamma_local = 1, gamma_below =  1, gamma_above = -1)  
    eur_format = eur.create_orders_df()
    eur_export = eur.format_create_orders_df('SG2','xls') 

    
    jpy = CreateOrders(merged_g10, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG2', Ccy1 = 'USD', Ccy2 = 'JPY', \
                      FixedCcy = 'USD', Tenor = 'SPT', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
                      amount_basic = 700000, odas_below = 2, odas_above = 2, gamma_local = 1, gamma_below =  1, gamma_above = -1) 

    jpy_format = jpy.create_orders_df()
    jpy_export = jpy.format_create_orders_df('SG2','xls') 


    cnh = CreateOrders(merged_asia, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG1', Ccy1 = 'USD', Ccy2 = 'CNH', \
                      FixedCcy = 'USD', Tenor = 'SPT', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
                      amount_basic = 1e6, odas_below = 2, odas_above = 3, gamma_local = 1, gamma_below =  1, gamma_above = -1) 

    cnh_format = cnh.create_orders_df()
    cnh_export = cnh.format_create_orders_df('SG1','xls') 

    

    php = CreateOrders(merged_asia, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG1', Ccy1 = 'USD', Ccy2 = 'PHP', \
                      FixedCcy = 'USD', Tenor = '1M', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
                      amount_basic = 1e6, odas_below = 3, odas_above = 3, gamma_local = 1, gamma_below =  1, gamma_above = -1)  
    php_format = php.create_orders_df()
    php_export = php.format_create_orders_df('ZSG1','xls')

    
    krw = CreateOrders(merged_asia, distance_from_techs = 0.2,  order_Type ='TP', Client = 'CP85VC', Account = 'FXOETFSG1', Ccy1 = 'USD', Ccy2 = 'KRW', \
                      FixedCcy = 'USD', Tenor = '1M', Activation = 'NOW', Expiry = '', Fixing = '', Comment_client = ''  , Comment_private = '',\
                      amount_basic = 800e3, odas_below = 0, odas_above = 3, gamma_local = 1, gamma_below =  1, gamma_above = -1) 

    krw_format = krw.create_orders_df()
    krw_export = krw.format_create_orders_df('SG1','xls')         
    #create report
    #env = Environment(loader = FileSystemLoader('.'))
    #template = env.get_template("MAs.html")#env is variable  we pass tempalte
    
    #template_vars = {"title": "support_resistance", 
     #                "MAs": df_signals.to_html() }
    #html_out = template.render(template_vars)
    
    #from weasyprint import HTML
    #HTML(string = html_out).write_pdf("report.pdf")
    
    
    '''
    #code block below: my stupid way to do it: isolate one by one from merged_asia, add temp columns for prices and stdev to create orders
    toy_df = merged_asia[merged_asia.loc[:,'Instru'].str.contains('NTN')]
    
    toy_df = toy_df[~toy_df.Instru.str.contains('HMA')]
    #https://stackoverflow.com/questions/61926275/pandas-row-operations-on-a-column-given-one-reference-value-on-a-different-col
    toy_df[['Fruit', 'Descr']] = toy_df['Instru'].str.split('_', n = 1, expand = True)
    #will use this as Index to use the df.at method later
    toy_df = toy_df.set_index('Descr')
    #first create odas near techs
    toy_df['Rate'] = (toy_df['VALUE'] * (1 - toy_df.at['realized_vol_unit','VALUE']* 0.2)).where(toy_df['Trend'] == 'Down',\
                                                                                                 toy_df['VALUE']*(1+toy_df.at['realized_vol_unit', 'VALUE']*0.2))
        
    toy_df.index = toy_df.index.fillna('no_label')
    print(toy_df.at['no_label', 'VALUE'])
    
    #then create odas based on stdev
    odas_above = 3
    odas_below = 2
    num_odas = odas_above + odas_below
    ##create num_odas new rows
    toy_df2 = pd.DataFrame([toy_df.loc['realized_vol_unit']] * num_odas)
      
    #then  create odas tech levels 
    counter_above = 1
    counter_below = 1
    for i in range(len(toy_df2)):
        
        #add odas above
        if i < odas_above:
            toy_df2['Rate'].iloc[i] = toy_df.at[None, 'VALUE'] * (1 + toy_df.at['realized_vol_unit', 'VALUE']*(counter_above))
            counter_above += 1
        else:
            toy_df2['Rate'].iloc[i] = toy_df.at[None, 'VALUE'] * (1 - toy_df.at['realized_vol_unit', 'VALUE']*(counter_below))
            counter_below += 1
            
    
    
    ##create num_odas new rows

    toy_df = toy_df.append(toy_df2)
    toy_df = toy_df.reset_index(drop = True).drop(columns = 'Fruit')
        
    
    #toy_df['temp'] = toy_df.VALUE.where(toy_df.Instru.str.contains('realized_vol_unit')).bfill()
    #toy_df['temp2'] = toy_df.VALUE.where(toy_df['Distance_%']== 0).ffill()
    #toy_df['rate'] = 0.0
    #toy_df = toy_df.sort_values('Instru', ascending = False )
    
    def reformat(grp):
        wrk = grp.set_index('Descr')
        print(wrk.columns)
        wrk['oda'] = wrk['VALUE'] - wrk.at['realized_vol_unit', 'VALUE']
        #wrk2 = pd.DataFrame([wrk.loc['realized_vol_unit']] *num_odas).assign(oda = [wrk.at[None, 'VALUE'] * (1 + wrk.at['realized_vol_unit', 'VALUE'] * i) for i in range(num_odas, 0, -1)])
        return wrk#pd.concat([wrk, wrk2])
    
    toy_df = toy_df.apply(reformat)
    toy_df2 = toy_df.apply(reformat(toy_df))'''
 
    
