import pandas as pd
import json 
import os 


class DataExtract:
    def __init__(self):
        pass

    def agg_transaction_data_extract(self,file_path):

        #This is to direct the path to get the data as states

            # path="/content/pulse/data/aggregated/transaction/country/india/state/"
            path=file_path
            Agg_state_list=os.listdir(path)
            Agg_state_list
            # print(f"The aggregated state list is : {Agg_state_list}")
            #Agg_state_list--> to get the list of states in India

            #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

            #This is to extract the data's to create a dataframe

            clm={'State':[], 'Year':[],'Quarter':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

            for i in Agg_state_list:
                p_i=path+i+"\\"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    p_j=p_i+j+"\\"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        for z in D['data']['transactionData']:
                            Name=z['name']
                            count=z['paymentInstruments'][0]['count']
                            amount=z['paymentInstruments'][0]['amount']
                            clm['Transacion_type'].append(Name)
                            clm['Transacion_count'].append(count)
                            clm['Transacion_amount'].append(amount)
                            clm['State'].append(i)
                            clm['Year'].append(j)
                            clm['Quarter'].append(int(k.strip('.json')))
            #Succesfully created a dataframe
            Agg_Trans=pd.DataFrame(clm)

            return Agg_Trans
    

    
    def agg_user_data_extract(self,file_path):

            #This is to direct the path to get the data as states

                # path="/content/pulse/data/aggregated/transaction/country/india/state/"
                path=file_path
                Agg_state_list=os.listdir(path)
                Agg_state_list
                # print(f"The aggregated state list is : {Agg_state_list}")
                #Agg_state_list--> to get the list of states in India

                #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

                #This is to extract the data's to create a dataframe

                clm={'State':[], 'Year':[],'Quarter':[],'user_brand':[], 'user_count':[], 'user_percentage':[]}
                clm_agg_app_open={'State':[], 'Year':[],'Quarter':[],'reg_users':[], 'app_opens':[]}

                for i in Agg_state_list:
                    # print(f"====================Starting Processing of state {i}=======================")
                    p_i=path+i+"\\"
                    Agg_yr=os.listdir(p_i)
                    for j in Agg_yr:
                        # print(f"====================Starting Processing of year {j}=======================")
                        p_j=p_i+j+"\\"
                        Agg_yr_list=os.listdir(p_j)
                        for k in Agg_yr_list:
                            # print(f"====================Starting Processing of json {k}=======================")
                            p_k=p_j+k
                            Data=open(p_k,'r')
                            D=json.load(Data)
                            # print(D['data']['usersByDevice'])
                            # print(type(D['data']['usersByDevice']))
                            clm_agg_app_open['State'].append(i)
                            clm_agg_app_open['Year'].append(j)
                            clm_agg_app_open['Quarter'].append(int(k.strip('.json')))
                            clm_agg_app_open['reg_users'].append(D['data']['aggregated']['registeredUsers'])
                            clm_agg_app_open['app_opens'].append(D['data']['aggregated']['appOpens'])
                            if D['data']['usersByDevice'] is not None:
                                for z in D['data']['usersByDevice']:
                                    brand=z['brand']
                                    count=z['count']
                                    percentage=z['percentage']
                                    clm['user_brand'].append(brand)
                                    clm['user_count'].append(count)
                                    clm['user_percentage'].append(percentage)
                                    clm['State'].append(i)
                                    clm['Year'].append(j)
                                    clm['Quarter'].append(int(k.strip('.json')))
                            # else:
                            #     clm['user_brand'].append(None)
                            #     clm['user_count'].append(None)
                            #     clm['user_percentage'].append(None)
                            #     clm['State'].append(i)
                            #     clm['Year'].append(j)
                            #     clm['Quarter'].append(int(k.strip('.json')))
                #Succesfully created a dataframe
                Agg_User=pd.DataFrame(clm)
                Agg_User_app_open=pd.DataFrame(clm_agg_app_open)

                return Agg_User,Agg_User_app_open
    

    def map_transaction_data_extract(self,file_path):

        #This is to direct the path to get the data as states

            # path="/content/pulse/data/aggregated/transaction/country/india/state/"
            path=file_path
            Agg_state_list=os.listdir(path)
            Agg_state_list
            # print(f"The aggregated state list is : {Agg_state_list}")
            #Agg_state_list--> to get the list of states in India

            #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

            #This is to extract the data's to create a dataframe

            clm={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'total_number_of_transactions':[], 'total_transaction_value':[]}

            for i in Agg_state_list:
                # print(f"====================Starting Processing of state {i}=======================")
                p_i=path+i+"\\"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    # print(f"====================Starting Processing of year {j}=======================")
                    p_j=p_i+j+"\\"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        # print(f"====================Starting Processing of json {k}=======================")
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        # print(D['data']['usersByDevice'])
                        # print(type(D['data']['usersByDevice']))
                        if D['data']['hoverDataList'] is not None:
                            for z in D['data']['hoverDataList']:
                                district_name=z['name']
                                total_number_of_transactions=z['metric'][0]['count']
                                total_transaction_value=z['metric'][0]['amount']
                                clm['district_name'].append(district_name)
                                clm['total_number_of_transactions'].append(total_number_of_transactions)
                                clm['total_transaction_value'].append(total_transaction_value)
                                clm['State'].append(i)
                                clm['Year'].append(j)
                                clm['Quarter'].append(int(k.strip('.json')))
            #Succesfully created a dataframe
            Agg_User=pd.DataFrame(clm)

            return Agg_User
    

    def map_user_data_extract(self,file_path):

        #This is to direct the path to get the data as states

            # path="/content/pulse/data/aggregated/transaction/country/india/state/"
            path=file_path
            Agg_state_list=os.listdir(path)
            Agg_state_list
            # print(f"The aggregated state list is : {Agg_state_list}")
            #Agg_state_list--> to get the list of states in India

            #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

            #This is to extract the data's to create a dataframe

            clm={'State':[], 'Year':[],'Quarter':[],'district_name':[], 'number_of_reg_users':[], 'total_app_opens':[]}

            for i in Agg_state_list:
                # print(f"====================Starting Processing of state {i}=======================")
                p_i=path+i+"\\"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    # print(f"====================Starting Processing of year {j}=======================")
                    p_j=p_i+j+"\\"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        # print(f"====================Starting Processing of json {k}=======================")
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        # print(D['data']['usersByDevice'])
                        # print(type(D['data']['usersByDevice']))
                        if D['data']['hoverData'] is not None:
                            for key, value in D['data']['hoverData'].items():
                                district_name=key
                                number_of_reg_users=value['registeredUsers']
                                total_app_opens=value['appOpens']
                                clm['district_name'].append(district_name)
                                clm['number_of_reg_users'].append(number_of_reg_users)
                                clm['total_app_opens'].append(total_app_opens)
                                clm['State'].append(i)
                                clm['Year'].append(j)
                                clm['Quarter'].append(int(k.strip('.json')))
            #Succesfully created a dataframe
            Agg_User=pd.DataFrame(clm)

            return Agg_User
    

    def top_transaction_data_extract(self,file_path):

        #This is to direct the path to get the data as states

            # path="/content/pulse/data/aggregated/transaction/country/india/state/"
            path=file_path
            Agg_state_list=os.listdir(path)
            Agg_state_list
            # print(f"The aggregated state list is : {Agg_state_list}")
            #Agg_state_list--> to get the list of states in India

            #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

            #This is to extract the data's to create a dataframe

            clm_states={'State':[], 'Year':[],'Quarter':[],'state_name':[],'total_transactions_count':[],'total_transactions_amount':[]}
            clm_districts={'State':[], 'Year':[],'Quarter':[],'district_name':[],'total_transactions_count':[],'total_transactions_amount':[]}
            clm_pincodes={'State':[], 'Year':[],'Quarter':[],'pincode':[],'total_transactions_count':[],'total_transactions_amount':[]}

            for i in Agg_state_list:
                # print(f"====================Starting Processing of state {i}=======================")
                p_i=path+i+"\\"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    # print(f"====================Starting Processing of year {j}=======================")
                    p_j=p_i+j+"\\"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        # print(f"====================Starting Processing of json {k}=======================")
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        # print(D['data']['usersByDevice'])
                        # print(type(D['data']['usersByDevice']))
                        if D['data'] is not None:
                            for key, value in D['data'].items():
                                # print(key)
                                # print(value)
                                if value is not None and key=='states':
                                    for items in value:
                                        state_name=items['entityName']
                                        total_transactions_count=items['metric']['count']
                                        total_transactions_amount=items['metric']['amount']
                                        clm_states['state_name'].append(state_name)
                                        clm_states['total_transactions_count'].append(total_transactions_count)
                                        clm_states['total_transactions_amount'].append(total_transactions_amount)
                                        clm_states['State'].append(i)
                                        clm_states['Year'].append(j)
                                        clm_states['Quarter'].append(int(k.strip('.json')))
                                elif value is not None and key=='districts':
                                    for items in value:
                                        district_name=items['entityName']
                                        district_transactions_count=items['metric']['count']
                                        district_transactions_amount=items['metric']['amount']
                                        clm_districts['district_name'].append(district_name)
                                        clm_districts['total_transactions_count'].append(district_transactions_count)
                                        clm_districts['total_transactions_amount'].append(district_transactions_amount)
                                        clm_districts['State'].append(i)
                                        clm_districts['Year'].append(j)
                                        clm_districts['Quarter'].append(int(k.strip('.json')))
                                elif value is not None and key=='pincodes':
                                    for items in value:
                                        pin_code=items['entityName']
                                        pincode_transactions_count=items['metric']['count']
                                        pincode_transactions_amount=items['metric']['amount']
                                        clm_pincodes['pincode'].append(pin_code)
                                        clm_pincodes['total_transactions_count'].append(pincode_transactions_count)
                                        clm_pincodes['total_transactions_amount'].append(pincode_transactions_amount)
                                        clm_pincodes['State'].append(i)
                                        clm_pincodes['Year'].append(j)
                                        clm_pincodes['Quarter'].append(int(k.strip('.json')))
            #Succesfully created a dataframe
            top_states=pd.DataFrame(clm_states)
            top_districts=pd.DataFrame(clm_districts)
            top_pincodes=pd.DataFrame(clm_pincodes)

            return top_states,top_districts,top_pincodes
    

    def top_user_data_extract(self,file_path):

        #This is to direct the path to get the data as states

            # path="/content/pulse/data/aggregated/transaction/country/india/state/"
            path=file_path
            Agg_state_list=os.listdir(path)
            Agg_state_list
            # print(f"The aggregated state list is : {Agg_state_list}")
            #Agg_state_list--> to get the list of states in India

            #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

            #This is to extract the data's to create a dataframe

            clm_states={'State':[], 'Year':[],'Quarter':[],'state_name':[],'registered_users_count':[]}
            clm_districts={'State':[], 'Year':[],'Quarter':[],'district_name':[],'registered_users_count':[]}
            clm_pincodes={'State':[], 'Year':[],'Quarter':[],'pincode':[],'registered_users_count':[]}

            for i in Agg_state_list:
                # print(f"====================Starting Processing of state {i}=======================")
                p_i=path+i+"\\"
                Agg_yr=os.listdir(p_i)
                for j in Agg_yr:
                    # print(f"====================Starting Processing of year {j}=======================")
                    p_j=p_i+j+"\\"
                    Agg_yr_list=os.listdir(p_j)
                    for k in Agg_yr_list:
                        # print(f"====================Starting Processing of json {k}=======================")
                        p_k=p_j+k
                        Data=open(p_k,'r')
                        D=json.load(Data)
                        # print(D['data']['usersByDevice'])
                        # print(type(D['data']['usersByDevice']))
                        if D['data'] is not None:
                            for key, value in D['data'].items():
                                # print(key)
                                # print(value)
                                if value is not None and key=='states':
                                    for items in value:
                                        state_name=items['name']
                                        registered_users_count=items['metric']['count']
                                        # registered_users_amount=items['metric']['amount']
                                        clm_states['state_name'].append(state_name)
                                        clm_states['registered_users_count'].append(registered_users_count)
                                        # clm_states['registered_users_amount'].append(registered_users_amount)
                                        clm_states['State'].append(i)
                                        clm_states['Year'].append(j)
                                        clm_states['Quarter'].append(int(k.strip('.json')))
                                elif value is not None and key=='districts':
                                    for items in value:
                                        district_name=items['name']
                                        registered_users_count=items['registeredUsers']
                                        # registered_users_amount=items['metric']['amount']
                                        clm_districts['district_name'].append(district_name)
                                        clm_districts['registered_users_count'].append(registered_users_count)
                                        # clm_districts['registered_users_amount'].append(registered_users_amount)
                                        clm_districts['State'].append(i)
                                        clm_districts['Year'].append(j)
                                        clm_districts['Quarter'].append(int(k.strip('.json')))
                                elif value is not None and key=='pincodes':
                                    for items in value:
                                        pin_code=items['name']
                                        registered_users_count=items['registeredUsers']
                                        # registered_users_amount=items['metric']['amount']
                                        clm_pincodes['pincode'].append(pin_code)
                                        clm_pincodes['registered_users_count'].append(registered_users_count)
                                        # clm_pincodes['registered_users_amount'].append(registered_users_amount)
                                        clm_pincodes['State'].append(i)
                                        clm_pincodes['Year'].append(j)
                                        clm_pincodes['Quarter'].append(int(k.strip('.json')))
            #Succesfully created a dataframe
            top_states=pd.DataFrame(clm_states)
            top_districts=pd.DataFrame(clm_districts)
            top_pincodes=pd.DataFrame(clm_pincodes)

            return top_states,top_districts,top_pincodes



if __name__=="__main__":
    DataExtract()