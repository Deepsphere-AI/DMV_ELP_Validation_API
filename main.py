"""
-----------------------------------------------------------------------------------------------------------------------------------------------------

© Copyright 2022, California, Department of Motor Vehicle, all rights reserved.
The source code and all its associated artifacts belong to the California Department of Motor Vehicle (CA, DMV), and no one has any ownership
and control over this source code and its belongings. Any attempt to copy the source code or repurpose the source code and lead to criminal
prosecution. Don't hesitate to contact DMV for further information on this copyright statement.

Release Notes and Development Platform:
The source code was developed on the Google Cloud platform using Google Cloud Functions serverless computing architecture. The Cloud
Functions gen 2 version automatically deploys the cloud function on Google Cloud Run as a service under the same name as the Cloud
Functions. The initial version of this code was created to quickly demonstrate the role of MLOps in the ELP process and to create an MVP. Later,
this code will be optimized, and Python OOP concepts will be introduced to increase the code reusability and efficiency.
____________________________________________________________________________________________________________
Development Platform                | Developer       | Reviewer   | Release  | Version  | Date
____________________________________|_________________|____________|__________|__________|__________________
Google Cloud Serverless Computing   | DMV Consultant  | Ajay Gupta | Initial  | 1.0      | 09/18/2022

-----------------------------------------------------------------------------------------------------------------------------------------------------

"""


import json
import traceback
import os
import time
from google.cloud import bigquery
from datetime import datetime

from DMV_ELP_Request_PreValidation import Pre_Request_Validation
from DMV_ELP_Public_Profanity_Validation import Profanity_Words_Check
from DMV_ELP_GuideLine_FWord_Validation import FWord_Validation
from DMV_ELP_Previously_Denied_Config_Validation import Previously_Denied_Configuration_Validation
from DMV_ELP_Pattern_Denial import Pattern_Denial

from DMV_ELP_BERT_Model_Prediction import BERT_Model_Result
from DMV_ELP_LSTM_Model_Prediction import LSTM_Model_Result

def ELP_Validation(request):
    
    vAR_partial_file_name = ""
    vAR_partial_file_date = ""
    vAR_error_message = {}
    request_json = request.get_json()
    print('request_json - ',request_json)
    print('request_json type- ',type(request_json))
    
    response_json = {}
    
    
    try:
        # To resolve container error(TypeError: Descriptors cannot not be created directly)
        os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION']='python'

        
        
        vAR_result_message = ""
        
        vAR_error_message = Pre_Request_Validation(request_json)



        if len(vAR_error_message["Error Message"])==0:
            vAR_input_text = request_json['LICENSE_PLATE_CONFIG'].upper()


            if request_json["FUNCTION"]==0:

                # Profanity Words Check
                vAR_configuration = vAR_input_text
                vAR_profanity_result,vAR_result_message = Profanity_Words_Check(vAR_input_text)

                if not vAR_profanity_result:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Approved"
                elif vAR_profanity_result:
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_result_message
                    return response_json
                    
                # FWord Validation
                vAR_fword_flag,vAR_fword_validation_message = FWord_Validation(vAR_input_text)

                if (vAR_fword_flag):
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_fword_validation_message
                    return response_json
                elif not vAR_fword_flag:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Approved"
                    
                # Previously denied configuration
                vAR_pdc_flag,vAR_previously_denied_validation_message = Previously_Denied_Configuration_Validation(vAR_input_text)
                

                if (vAR_pdc_flag):
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_previously_denied_validation_message
                    return response_json
                
                elif not vAR_pdc_flag:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Approved"
                
                # Pattern denial
                vAR_regex_result,vAR_pattern = Pattern_Denial(vAR_input_text)


                if not vAR_regex_result:
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = " Similar to " +vAR_pattern+ " Pattern"
                    return response_json
                    
                elif vAR_regex_result:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Approved"

                # # Model prediction

                # vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input_text)
				
                # vAR_result_data = str(vAR_result_data.to_json(orient='records'))

                # if not vAR_result:
                #     vAR_recommendation = "Denied"
                #     vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
                #     response_json["ResponseCode"] = vAR_recommendation
                #     response_json["Reason"] = vAR_recommendation_reason
                # else:
                #     vAR_recommendation = "Approved"
                #     response_json["Reason"] = ""
                #     response_json["ResponseCode"] = vAR_recommendation
                
                return response_json

            elif request_json["FUNCTION"]==1:
                # Profanity check
                # vAR_start_time = datetime.now()
                # vAR_function_name = "Profanity_Words_Check"
                vAR_configuration = vAR_input_text
                vAR_profanity_result,vAR_result_message = Profanity_Words_Check(vAR_input_text)
    

                if not vAR_profanity_result:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Approved"

                    # Insert Record
                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json


                elif vAR_profanity_result:
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_result_message
                    # response_json["MODEL"] = None
                    # response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                    # "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                    
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    
                    return response_json

            elif request_json["FUNCTION"]==2:
                # FWord Guideline check
                # vAR_start_time = datetime.now()
                # vAR_function_name = "FWord_Validation"
                vAR_configuration = vAR_input_text
                vAR_fword_flag,vAR_fword_validation_message = FWord_Validation(vAR_input_text)

    

                if (vAR_fword_flag):
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_fword_validation_message
                    # response_json["MODEL"] = None
                    # response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                    # "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                    
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)

                    return response_json
                elif not vAR_fword_flag:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Accepted"
                    
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json



            elif request_json["FUNCTION"]==3:
                # Previously Denied configuration check
                # vAR_start_time = datetime.now()
                # vAR_function_name = "Previously_Denied_Configuration_Validation"
                vAR_configuration = vAR_input_text
                vAR_pdc_flag,vAR_previously_denied_validation_message = Previously_Denied_Configuration_Validation(vAR_input_text)
                

                if (vAR_pdc_flag):
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = vAR_previously_denied_validation_message
                    # response_json["MODEL"] = None
                    # response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                    # "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json
                elif not vAR_pdc_flag:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Accepted"
                
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json
            elif request_json["FUNCTION"]==4:
                # Pattern denial check
                # vAR_start_time = datetime.now()
                # vAR_function_name = "Pattern_Denial"
                vAR_configuration = vAR_input_text
                vAR_regex_result,vAR_pattern = Pattern_Denial(vAR_input_text)


                if not vAR_regex_result:
                    
                    response_json["ResponseCode"] = "Denied"
                    response_json["Reason"] = " Similar to " +vAR_pattern+ " Pattern"
                    # response_json["MODEL"] = None
                    # response_json["MODEL_PREDICTION"] = {"PROFANITY_CLASSIFICATION":[{"IDENTITY_HATE":None,"INSULT":None,"OBSCENE":None,"SEVERE_TOXIC":None,
                    # "THREAT":None,"TOXIC":None}],"SUM_OF_ALL_CATEGORIES":None}
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json

                elif vAR_regex_result:
                    response_json["Reason"] = ""
                    response_json["ResponseCode"] = "Accepted"
                    # Insert Record

                    # vAR_endtime = datetime.now()
                    # vAR_duration = vAR_endtime - vAR_start_time
                    # # Load client
                    # client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
                    # table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_MLOPS_RUNTIME_STAT'
                    # vAR_query = """insert into `{}` (FUNCTION_NAME,CONFIGURATION,STARTTIME,ENDTIME,DURATION) values("{}","{}","{}","{}","{}")""".format(table,vAR_function_name,vAR_configuration,vAR_start_time,vAR_endtime,vAR_duration)
                    # print('Query - ',vAR_query)
                    # vAR_job = client.query(vAR_query)
                    # vAR_job.result()
                    # vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
                    # print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
                    return response_json

            # elif request_json["FUNCTION"]==5:
            #     vAR_configuration = vAR_input_text
            #     vAR_result,vAR_result_data,vAR_result_target_sum = BERT_Model_Result(vAR_input_text)
				
            #     vAR_result_data = str(vAR_result_data.to_json(orient='records'))

            #     if not vAR_result:
            #         vAR_recommendation = "Denied"
            #         vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
            #         response_json["ResponseCode"] = vAR_recommendation
            #         response_json["Reason"] = vAR_recommendation_reason
            #     else:
            #         vAR_recommendation = "Approved"
					
            #         response_json["ResponseCode"] = vAR_recommendation
            #         response_json["Reason"] = ""
			# 	    # response_json["Model Prediction"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum)}
				
            #     return response_json
            
            # elif request_json["FUNCTION"]==6:
            #     vAR_configuration = vAR_input_text
            #     vAR_result,vAR_result_data,vAR_result_target_sum = LSTM_Model_Result(vAR_input_text)
				
            #     vAR_result_data = str(vAR_result_data.to_json(orient='records'))

            #     if not vAR_result:
            #         vAR_recommendation = "Denied"
            #         vAR_recommendation_reason = "Highest Profanity category probability should be below the threshold value(0.5)"
            #         response_json["ResponseCode"] = vAR_recommendation
            #         response_json["Reason"] = vAR_recommendation_reason
            #     else:
            #         vAR_recommendation = "Approved"
					
            #         response_json["ResponseCode"] = vAR_recommendation
            #         response_json["Reason"] = ""
			# 	    # response_json["Model Prediction"] = {"PROFANITY_CLASSIFICATION":json.loads(str(vAR_result_data)),"SUM_OF_ALL_CATEGORIES":float(vAR_result_target_sum)}
				
            #     return response_json
            
            else:
                response_json["ErrorDescription"] = "Invalid Function Value"
                response_json["ErrorCode"] = 101
                return response_json

        else:
            response_json["ErrorDescription"] = vAR_error_message["Error Message"]
            response_json["ErrorCode"] = 102
            return response_json

    except BaseException as e:
        print('In Error Block - '+str(e))
        print('Error Traceback4 - '+str(traceback.print_exc()))
        response_json["ErrorDescription"] = '### '+str(e)
        response_json["ErrorCode"] = 103
        return response_json
