import os
import logging
from datetime import datetime
import sys
# Add the parent directory to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print("The parent directory is :", parent_dir)
sys.path.append(parent_dir)

from Data_Extraction import data_extract as d
from state_mapping import state_mapping


## Setting up the logging 
# Create a log folder if it doesn't exist
if not os.path.exists(f"{parent_dir}/logs"):
    os.makedirs(f"{parent_dir}/logs")

# Get current date and time
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure logging with the dynamic log file name
log_file_name = f"{parent_dir}/logs/log_file_{current_datetime}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file_name
)


def extract_data():
    try:
        #setting up a directory folder to store the processed dataframes
        directory="process_dataframe"
        if not os.path.exists(directory):
            os.makedirs(directory)
        # Creating a sample dictionary and starting the data extraction process
        data_dict=dict()
        agg_trans_obj=d.DataExtract()
        logging.info("Starting aggregate transaction data processing")
        agg_trans=agg_trans_obj.agg_transaction_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\aggregated\\transaction\\country\\india\\state\\")
        agg_trans=state_mapping(agg_trans)
        data_dict['agg_trans']=agg_trans
        agg_trans.to_csv(os.path.join(directory,'agg_trans.csv'),index=False)
        logging.info("Aggregate transaction data processing completed")
        logging.info("Starting aggregate user data processing")
        agg_user,agg_user_app_open=agg_trans_obj.agg_user_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\aggregated\\user\\country\\india\\state\\")
        agg_user=state_mapping(agg_user)
        agg_user_app_open=state_mapping(agg_user_app_open)
        data_dict['agg_user']=agg_user
        data_dict['agg_user_app_open']=agg_user_app_open
        agg_user.to_csv(os.path.join(directory,'agg_user.csv'),index=False)
        agg_user_app_open.to_csv(os.path.join(directory,'agg_user_app_open.csv'),index=False)
        logging.info("Completed aggregate user data processing")
        logging.info("Starting map transaction data processing")
        map_trans=agg_trans_obj.map_transaction_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\map\\transaction\\hover\\country\\india\\state\\")
        map_trans=state_mapping(map_trans)
        data_dict['map_trans']=map_trans
        map_trans.to_csv(os.path.join(directory,'map_trans.csv'),index=False)
        logging.info("Completed map transaction data processing")
        logging.info("Starting map user data processing")
        map_user=agg_trans_obj.map_user_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\map\\user\\hover\\country\\india\\state\\")
        map_user=state_mapping(map_user)
        data_dict['map_user']=map_user
        map_user.to_csv(os.path.join(directory,'map_user.csv'),index=False)
        logging.info("Completed map user data processing")
        logging.info("Starting top transaction data processing")
        top_tran_state,top_tran_dist,top_tran_pincode=agg_trans_obj.top_transaction_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\top\\transaction\\country\\india\\state\\")
        top_tran_state=state_mapping(top_tran_state)
        top_tran_pincode=state_mapping(top_tran_pincode)
        top_tran_dist=state_mapping(top_tran_dist)
        data_dict['top_trans_state']=top_tran_state
        data_dict['top_trans_dist']=top_tran_dist
        data_dict['top_trans_pincode']=top_tran_pincode
        top_tran_state.to_csv(os.path.join(directory,'top_tran_states.csv'),index=False)
        top_tran_dist.to_csv(os.path.join(directory,'top_tran_districts.csv'),index=False)
        top_tran_pincode.to_csv(os.path.join(directory,'top_tran_pincodes.csv'),index=False)
        logging.info("Completed top transaction processing")
        logging.info("Starting top user data processing")
        top_usr_state,top_usr_dist,top_usr_pincode=agg_trans_obj.top_user_data_extract("D:\\Saravanesh Personal\\Guvi\\Capstone Projects\\Phonepe\\Data_Extraction\\Data\\data\\top\\user\\country\\india\\state\\")
        top_usr_state=state_mapping(top_usr_state)
        top_usr_dist=state_mapping(top_usr_dist)
        top_usr_pincode=state_mapping(top_usr_pincode)
        data_dict['top_usr_state']=top_usr_state
        data_dict['top_usr_dist']=top_usr_dist
        data_dict['top_usr_pincode']=top_usr_pincode
        top_usr_state.to_csv(os.path.join(directory,'top_usr_states.csv'),index=False)
        top_usr_dist.to_csv(os.path.join(directory,'top_usr_districts.csv'),index=False)
        top_usr_pincode.to_csv(os.path.join(directory,'top_usr_pincodes.csv'),index=False)
        logging.info("Completed top user data processing")
    except Exception as e:
        logging.error(f"Encountered an expection : {e}")

    return data_dict


if __name__=="__main__":
    extract_data()