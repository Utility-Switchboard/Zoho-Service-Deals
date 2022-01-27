from zcrmsdk.src.com.zoho.crm.api.record import *
from zcrmsdk.src.com.zoho.crm.api.record.action_wrapper import ActionWrapper
from zcrmsdk.src.com.zoho.crm.api.record.success_response import SuccessResponse
from zcrmsdk.src.com.zoho.crm.api.record.api_exception import APIException
from zcrmsdk.src.com.zoho.crm.api.record.body_wrapper import BodyWrapper
from zcrmsdk.src.com.zoho.crm.api.record import Record as ZCRMRecord
from zcrmsdk.src.com.zoho.crm.api.exception import SDKException
from zcrmsdk.src.com.zoho.crm.api.users import User
from zcrmsdk.src.com.zoho.crm.api.layouts import Layout
from zcrmsdk.src.com.zoho.crm.api.util import Choice
# from zcrmsdk.src.com.zoho.crm.api.notes import *
# from zcrmsdk.src.com.zoho.crm.api.notes import BodyWrapper as BodyWrap
# from zcrmsdk.src.com.zoho.crm.api.notes.note import Note as ZCRMNote
# from zcrmsdk.src.com.zoho.crm.api import ParameterMap

from datetime import datetime
import psycopg2

from init import SDKInitializer

from flask import Flask, request, json
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*":{"origins":"*"}})
app.secret_key = "TheBoilerCompany"

def dbConnection():
    host = "backup-server-restore.postgres.database.azure.com"
    dbname = "USBCrm"
    user = "usb@backup-server-restore"
    password = "postgres220-"
    sslmode = "prefer"
    database_uri ="host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    connection = psycopg2.connect(database_uri)

    return connection

def postFailure(obj, exc):
        print('insert_zoho_failure')
        query = "INSERT INTO usb.zoho_failures(json_entry, json_error, trace_log, service_number) VALUES(%s, %s, %s, %s)"

        con = dbConnection()
        cursor = con.cursor()
        try:
            cursor.execute(query,(json.dumps(obj).replace("'","''"), json.dumps(exc), str(exc['Message']), obj['service_number']))
            con.commit()
        except Exception as ex:
            print(str(ex))
            return {'status': 100, 'error': str(ex)}
        finally:
            cursor.close()
            con.close()

        res = {
            'status' : 500,
            'message' : 'Deal added to zoho_failures table.'
        }
        return res

def delete(recordID, module):
        
    try:
        # Get instance of RecordOperations Class
        record_operations = RecordOperations()
        
        # Call deleteRecord method that takes param_instance, module_api_name and record_id as parameter.
        response = record_operations.delete_record(recordID, module)

        if response is not None:

            # Get object from response
            response_object = response.get_object()

            if response_object is not None:

                # Check if expected ActionWrapper instance is received.
                if isinstance(response_object, ActionWrapper):

                    # Get the list of obtained ActionResponse instances
                    action_response_list = response_object.get_data()

                    for action_response in action_response_list:

                        # Check if the request is successful
                        if isinstance(action_response, SuccessResponse):

                            # Get the details dict
                            details = action_response.get_details()

                            res = {
                                'status': action_response.get_code().get_value()
                            }
                            return res


    except SDKException as ex:

        err = {
            'status' : 500,
            'details' : ex.details,
            'trace' : str(ex.with_traceback(ex.__traceback__))
        }
        return err

    except Exception as ex:

        err = {
            'status' : 500,
            'details' : str(ex),
            'trace' : None
        }
        return err
        
def createCustomerAddress(obj, layout, uId):
    print(obj)
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()

    # List to hold Record instances
    records_list = []

    # Get instance of Record Class
    record = ZCRMRecord()

    owner = User()
    owner.set_id(uId)
    uLayout = Layout()
    uLayout.set_id(layout)
    if obj['address_1'] is not None and obj["address_1"] != "":
        record.add_key_value("Address_1", '{}'.format(obj["address_1"]))
    if obj['address_2'] is not None and obj["address_2"] != "":
        record.add_key_value("Address_2", '{}'.format(obj["address_2"]))
    if obj['city'] is not None and obj["city"] != "":
        record.add_key_value("Town", '{}'.format(obj["city"]))
    if obj['postcode'] is not None and obj["postcode"] != "":
        record.add_key_value("Name", '{}'.format(obj["postcode"]))
    if obj['door_number'] is not None and obj["door_number"] != "":
        record.add_key_value("DoorNumber", '{}'.format(obj["door_number"]))
    record.add_key_value("Owner", owner)
    record.add_key_value("Layout", uLayout)


    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    
    trigger = []

    # Set the list containing the trigger operations to be run
    request.set_trigger(trigger)

    # Call create_records method that takes BodyWrapper instance and module_api_name as parameters
    response = record_operations.create_records('Customers_Profile', request)

    if response is not None:
        cAddress = {}

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:
            
            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):

                        # Get the details dict
                        details = action_response.get_details()
                        return {
                            'status': 200,
                            'addId': details['id']
                        }
                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):

                        return {'status': response.get_status_code(),'Message' : action_response.get_message().get_value()}

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                return {'status': response.get_status_code(),'Message' : response_object.get_message().get_value()}
            
def createCustomer(obj, uId):
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()

    # List to hold Record instances
    records_list = []

    # Get instance of Record Class
    record = ZCRMRecord()

    owner = User()
    owner.set_id(uId)

    if obj['email'] is not None and obj['email'] != '':
        record.add_key_value("Email", '{}'.format(obj["email"] ))
    if obj['Salutation'] is not None and obj['Salutation'] != '':
        salutation = Choice(obj["Salutation"])
        record.add_key_value("Salutation", salutation)
    if obj["Last_Name"] is not None and obj['Last_Name'] != '':
        record.add_key_value("Last_Name", '{}'.format(obj["Last_Name"]))
    if obj["First_Name"] is not None and obj['First_Name'] != '':
        record.add_key_value("First_Name", '{}'.format(obj["First_Name"]))
    if obj['Phone'] is not None and obj['Phone'] != '':
        record.add_key_value("Phone", '{}'.format(obj["Phone"]))
    if obj['Mobile'] is not None and obj['Mobile'] != '':
        record.add_key_value("Mobile", '{}'.format(obj["Mobile"]))
    if obj["marketing"] is not None and obj["marketing"] != "":
        marketing = Choice(obj["marketing"])
        record.add_key_value("Marketing_Opt_In", marketing)
    record.add_key_value("Owner", owner)


    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    
    trigger = []

    # Set the list containing the trigger operations to be run
    request.set_trigger(trigger)

    # Call create_records method that takes BodyWrapper instance and module_api_name as parameters
    response = record_operations.create_records('Contacts', request)

    if response is not None:

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:
            
            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):

                        # Get the details dict
                        details = action_response.get_details()
                        return {
                            'status': 200,
                            'customId': details['id']
                        }
                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):

                        return {'status': response.get_status_code(),'Message' : action_response.get_message().get_value()}

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                return {'status': response.get_status_code(),'Message' : response_object.get_message().get_value()}

def createAccount(obj, uId, aId):
    
    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()

    # List to hold Record instances
    records_list = []

    # Get instance of Record Class
    record = ZCRMRecord()

    owner = User()
    owner.set_id(uId)
    
    address = ZCRMRecord()
    address.set_id(aId)
    
    if obj["account_name"] is not None and obj["account_name"] != '':
        record.add_key_value("Name", '{}'.format(obj["account_name"]))
    if obj["account_number"] is not None and obj["account_number"] != '':
        record.add_key_value("Account_Number", '{}'.format(obj["account_number"]))
    if obj["sort_code"] is not None and obj["sort_code"] != '':
        record.add_key_value("Sort_Code", '{}'.format(obj["sort_code"]))
    if obj["service_number"] is not None and obj["service_number"] != '':
        record.add_key_value("Service_Number", '{}'.format(obj["service_number"]))
    if obj['payment_reference'] is not None and obj["payment_reference"] != '':
        record.add_key_value("Payment_Reference", '{}'.format(obj["payment_reference"]))
    
    record.add_key_value("Address", address)
    record.add_key_value("Owner", owner)


    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    
    trigger = []

    # Set the list containing the trigger operations to be run
    request.set_trigger(trigger)

    # Call create_records method that takes BodyWrapper instance and module_api_name as parameters
    response = record_operations.create_records('Payments_Information', request)

    if response is not None:

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:
            
            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):

                        # Get the details dict
                        details = action_response.get_details()
                        return {
                            'status': 200,
                            'accId': details['id']
                        }
                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):

                        return {'status': response.get_status_code(),'Message' : action_response.get_message().get_value()}

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                return {'status': response.get_status_code(),'Message' : response_object.get_message().get_value()}

def setFields(obj, layout, record, uId, addId, cId, aId = None):
    owner = User()
    owner.set_id(uId)
    
    address = ZCRMRecord()
    address.set_id(addId)
    
    custom = ZCRMRecord()
    custom.set_id(cId)   
     
    uLayout = Layout()
    uLayout.set_id(layout)
    
    if obj["service_number"] is not None and obj["service_number"] != "":
        record.add_key_value("Name", '{}'.format(obj["service_number"]))
    
    
    d = datetime.strptime(datetime.strftime(datetime.today(), '%d/%m/%Y'), '%d/%m/%Y').date()
    print(d)
    record.add_key_value("Create_Date", d)
        
    if obj["price"] is not None and obj["price"] != "":
        record.add_key_value("Price", float(obj["price"]))
        
    if obj["policy_type"] is not None and obj["policy_type"] != "":
        servLevel = Choice(obj["policy_type"])
        record.add_key_value("Policy_Type", servLevel)
        
    if obj["payment_type"] is not None and obj["payment_type"] != "":
        servLevel = Choice(obj["payment_type"])
        record.add_key_value("Payment_Type", servLevel)
        
    
    record.add_key_value("Payment_Date", datetime.strptime(datetime.strftime(datetime.today(), '%d/%m/%Y'), '%d/%m/%Y').date())
        
    if obj["worldpay_auth_code"] is not None and obj["worldpay_auth_code"] != "":
        record.add_key_value("Worldpay_auth_code", obj["worldpay_auth_code"])
        
    if obj["one_off_stage"] is not None and obj["one_off_stage"] != "":
        oneOffStage = Choice(obj["one_off_stage"])
        record.add_key_value("One_off_Stage", oneOffStage)
        
    if obj["boiler_type"] is not None and obj["boiler_type"] != "":
        boilerType = Choice(obj["boiler_type"])
        record.add_key_value("Boiler_type", boilerType)
        
    if obj["has_hot_water"] is not None and obj["has_hot_water"] != "":
        hotWater = Choice(obj["has_hot_water"])
        record.add_key_value("Has_hot_water", hotWater)
        
    if obj["has_central_heating"] is not None and obj["has_central_heating"] != "":
        centralHeating = Choice(obj["has_central_heating"])
        record.add_key_value("Has_central_heating", centralHeating)
        
    if obj["boiler_fault_code"] is not None and obj["boiler_fault_code"] != "":
        record.add_key_value("Boiler_fault_code", obj["boiler_fault_code"])
        
    if obj["boiler_manufacturer"] is not None and obj["boiler_manufacturer"] != "":
        
        record.add_key_value("Boiler_manufacturer", f'{obj["boiler_manufacturer"]}')
        
    if obj["boiler_model"] is not None and obj["boiler_model"] != "":
        record.add_key_value("Boiler_model", obj["boiler_model"])
        
    if obj["boiler_age"] is not None and obj["boiler_age"] != "":
        record.add_key_value("Boiler_age_in_years", int(obj["boiler_age"]))
        
    if obj["boiler_issue_type"] is not None and obj["boiler_issue_type"] != "":
        record.add_key_value("Boiler_issue_type", obj["boiler_issue_type"])
        
    if obj["monday"] is not None and obj["monday"] != "":
        monday = Choice(obj["monday"])
        record.add_key_value("Monday", monday)
        
    if obj["tuesday"] is not None and obj["tuesday"] != "":
        tuesday = Choice(obj["tuesday"])
        record.add_key_value("Tuesday", tuesday)
        
    if obj["wednesday"] is not None and obj["wednesday"] != "":
        wednesday = Choice(obj["wednesday"])
        record.add_key_value("Wednesday", wednesday)
        
    if obj["thursday"] is not None and obj["thursday"] != "":
        thursday = Choice(obj["thursday"])
        record.add_key_value("Thursday", thursday)
        
    if obj["friday"] is not None and obj["friday"] != "":
        friday = Choice(obj["friday"])
        record.add_key_value("Friday", friday)

    if obj["company"] is not None and obj["company"] != "":
        record.add_key_value("Company", '{}'.format(obj["company"]))  

    record.add_key_value("Address", address)
    record.add_key_value("Owner", owner)
    record.add_key_value("Customer", custom)
    
    if aId is not None:
        print('account added')
        account = ZCRMRecord()
        account.set_id(aId)   
        record.add_key_value("Account", account)
        
    record.add_key_value("Layout", uLayout)
    
    return record

def createPolicy(obj, uId, cId, addId, aId = None):
    layout = 256952000002920006

    # Get instance of RecordOperations Class
    record_operations = RecordOperations()

    # Get instance of BodyWrapper Class that will contain the request body
    request = BodyWrapper()

    # List to hold Record instances
    records_list = []

    # Get instance of Record Class
    record = ZCRMRecord()
    
    record = setFields(obj, layout, record, uId, addId, cId, aId)

    records_list.append(record)
    # Set the list to data in BodyWrapper instance
    request.set_data(records_list)
    
    trigger = []

    # Set the list containing the trigger operations to be run
    request.set_trigger(trigger)

    # Call create_records method that takes BodyWrapper instance and module_api_name as parameters
    response = record_operations.create_records('Customers', request)

    if response is not None:

        # Get object from response
        response_object = response.get_object()

        if response_object is not None:
            
            # Check if expected ActionWrapper instance is received.
            if isinstance(response_object, ActionWrapper):

                # Get the list of obtained ActionResponse instances
                action_response_list = response_object.get_data()

                for action_response in action_response_list:

                    # Check if the request is successful
                    if isinstance(action_response, SuccessResponse):

                        # Get the details dict
                        details = action_response.get_details()
                        return {
                            'status' : 200,
                            'policyId': details['id']
                        }  
                    # Check if the request returned an exception
                    elif isinstance(action_response, APIException):

                        return {'status': response.get_status_code(),'Message' : action_response.get_message().get_value()}

            # Check if the request returned an exception
            elif isinstance(response_object, APIException):

                return {'status': response.get_status_code(),'Message' : response_object.get_message().get_value()}              

def main(obj):
    print('inside main method')
    addId, customId, accId, policyId = None, None, None, None
    try:
        SDKInitializer.initialize()
        userId = 256952000000244001
        custAddLayout = 256952000002851035
       
        addId = createCustomerAddress(obj['address'], custAddLayout, userId)
        print("add ", addId)
        if addId['status'] == 200:
            customId = createCustomer(obj['customer_info'], userId)
            print("custom ", customId)
            if customId['status'] == 200:
                if 'account' in obj:
                    accId = createAccount(obj['account'], userId, addId['addId'])
                    print('account ', accId)
                    if accId['status'] == 200:
                        policyId = createPolicy(obj, userId, customId['customId'], addId['addId'], accId['accId'])
                        if policyId['status'] == 200:
                            print('policy ', policyId)
                            return policyId
                        else:
                            policyId['method'] = 'createPolicy'
                            postFailure(obj, policyId)
                            deletedCustomer = delete(accId['accId'], 'Payments_Information') 
                            deletedCustomer = delete(customId['customId'], 'Contacts') 
                            deletedAddress = delete(addId['addId'], 'Customers_Profile') 
                            return policyId
                    else:
                        accId['method'] = 'createAccount'
                        postFailure(obj, accId)
                        deletedCustomer = delete(customId['customId'], 'Contacts') 
                        deletedAddress = delete(addId['addId'], 'Customers_Profile') 
                        return accId
                else:
                    policyId = createPolicy(obj, userId, customId['customId'], addId['addId'])
                    if policyId['status'] == 200:
                        print('policy ', policyId)
                        return policyId  
                    else:
                        policyId['method'] = 'createPolicy'
                        postFailure(obj, policyId)
                        deletedCustomer = delete(customId['customId'], 'Contacts') 
                        deletedAddress = delete(addId['addId'], 'Customers_Profile') 
                        return policyId
            else:
                customId['method'] = 'createCustomer'
                postFailure(obj, customId)
                deletedAddress = delete(addId['addId'], 'Customers_Profile')
                return customId
    
        else:
            addId['method'] = 'createCustomerAddress'
            postFailure(obj, addId)
            return addId
    except SDKException as ex:

        err = {
            'status' : 500,
            'Massage' : ex.details,
            'trace' : str(ex.with_traceback(ex.__traceback__))
        }
        postFailure(obj, err)
        return err
    
    except Exception as ex:
        err ={
            'code': 100,
            'Message': str(ex)
        }
        postFailure(obj, err)
        return err

    
@app.route('/CreateServicePolicy', methods=['POST'])
def createDeal():
    try:
        if request.method == 'POST':
            obj = json.loads(request.data.decode('utf-8'))
            # print(json.dumps(obj['data']).replace("'","''"))

            response = main(obj['data'])
            print(response)
            
            return response
    except Exception as ex:

        err ={
            'code': 100,
            'Message': str(ex)
        }
        postFailure(obj, err)
        return err

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='6002', debug=True)


# obj = {
#     "service_number": "OOW2-1410003",
#     "customer_info": {
#         "email": "jesus.sicairos-lopez@utilityswitchboad.com",
#         "Salutation": "Mr.",
#         "Last_Name": "Sicairos",
#         "First_Name": "Jesus",
#         "marketing": "Yes",
#         "Mobile": "07522468529",
#         "Phone": ""
#     },
#     "address": {
#         "address_1": "Flat 11,Maydwell House,Thomas Road,London,",
#         "address_2": "N/A",
#         "city": "London",
#         "postcode": "E14 7AP",
#         "door_number": "11",
#         "layout": ""
#     },
#     "price": 325,
#     "policy_type": "Fixed Price Repair",
#     "payment_date": "",
#     "payment_type": "",
#     "worldpay_auth_code": "OOW2-1400003-NFPR",
#     "company": "Smart Plan",
#     "create_date": "",
#     "one_off_stage": "Awaiting Action",
#     "boiler_type": "LGP",
#     "has_hot_water": "Yes",
#     "has_central_heating": "No",
#     "boiler_fault_code": "124585",
#     "boiler_manufacturer": "",
#     "boiler_model": "hjgy1254",
#     "boiler_age": 5,
#     "boiler_issue_type": "Don't Know",
#     "customer_unavailability": "",
#     "date_booked_in": "",
#     "engineer_s_name": "",
#     "engineer_cost": "",
#     "net_profit": "",
#     "monday": "None",
#     "tuesday": "AM",
#     "wednesday": "None",
#     "thursday": "None",
#     "friday": "PM"
# }
