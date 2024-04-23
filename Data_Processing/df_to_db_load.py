import logging
from json_to_df_processing import extract_data
from data_load_sql_connector import Dataload
from mysql.connector import IntegrityError
from json_to_df_processing import extract_data

def db_load(data_dict):
    try:
        print("The dataframe names provided:",data_dict.keys())
        for dataframe in data_dict: 
                if dataframe is not None:
                    # Display the DataFrame returned by the function
                    # st.write(dataframe)
                    try:
                        dl_ob=Dataload()
                        dl_ob.load_df(data_dict[dataframe],dataframe)
                        logging.info(f"{dataframe} Data Extracted, processed & loaded successfully")
                    except IntegrityError as e:
                        logging.error(f'Unique Key Violation while inserting the to the {dataframe} table. Provide another id')
                    except Exception as e:
                        logging.error(f"Error loading {dataframe} data to database {e}")
                else:
                    logging.error('Failed to retrieve channel details. Please check the Channel ID')
    except Exception as e:
        logging.error(f"An exception has occured : {e}")

if __name__=="__main__":
    db_load(data_dict=extract_data())