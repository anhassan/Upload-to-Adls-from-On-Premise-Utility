import glob
import os
import csv
import requests
import json
from datetime import datetime

def get_latest_file(dir_path):
	file_list = glob.glob(dir_path)
	latest_file = [max(file_list,key=os.path.getctime).replace("\\","/")] if len(file_list) > 0  else []
	return latest_file


def remove_all_files(dir_path):
	for file in glob.glob(dir_path):
		file_path = file.replace("\\","/")
		os.remove(file_path)
		print("Removed File : {} ".format(file_path))


def read_csv_file(file_path):
  content = ""
  with open(file_path) as file:
        reader = csv.reader(file,delimiter=",",quotechar='"')
        data = [",".join(row) for row in reader]
  for element in data:
    content += element + "\n"
  return content.replace("\ufeff","")[0:-1]


def get_access_token(client_secret,client_id,tenant_id):
    auth_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth_body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope" : "https://storage.azure.com/.default",
        "grant_type" : "client_credentials"
    }

    try:
    	response = requests.post("https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(tenant_id), headers=auth_headers, data=auth_body)
    	if response.status_code != 200:
    		raise Exception("Authentication Token Retreival Failed with Status Code : {}".format(response.status_code))
    except Exception as error:
    	print("Error : {}".format(error))
    	exit(1)

    return json.loads(response.content)["access_token"]


def upload_to_adls(params,file_path):
  access_token = get_access_token(params["client_secret"],params["client_id"],params["tenant_id"])
  headers={"Authorization": "Bearer {}".format(access_token)}
  
  file_name = file_path[file_path.rfind("/")+1:]
  url_create_file = "{}/{}/{}/{}?resource=file".format(params["adls_endpoint"],params["adls_root_dir"],params["adls_dest_dir"],file_name)
  
  try:
  	 response = requests.put(url_create_file,headers=headers)
  except Exception as error:
  	 print("Error with file creation of file : {} because : {}".format(file_name,error))
  	 exit(1)

  content = read_csv_file(file_path)

  append_file_headers = {
        "Authorization": "Bearer {}".format(access_token),
        'Content-Type': 'text/text; charset=utf-8',
        "Content-Length": "{}".format(len(content))
    }

  url_append_file = "{}/{}/{}/{}?action=append&position={}".format(params["adls_endpoint"],params["adls_root_dir"],params["adls_dest_dir"],file_name,0)

  try:
  	response = requests.patch(url_append_file, headers=append_file_headers, data=content.encode("utf-8"))
  except Exception as error:
  	print("Error during appending data into file : {} because : {}".format(file_name,error))
  	exit(1)

  url_flush_file = "{}/{}/{}/{}?action=flush&position={}".format(params["adls_endpoint"],params["adls_root_dir"],params["adls_dest_dir"],file_name,len(content))

  try:
  	response = requests.patch(url_flush_file, headers=headers)
  	print("File name : {} uploaded to Adls at time : {}".format(file_name,datetime.now()))
  except Exception as error:
  	print("Error during flushing content into file : {} because : {}".format(file_name,error))
  	exit(1)
    
  

if __name__ == "__main__":

	params = {
	"client_id" : "3c479dca-a03f-427c-ac3d-8087e99badde",
	"client_secret" : "mDlzp3-~161Lbz3.Tz~2vKUUvcSQ3RYvq1",
	"tenant_id" : "f1afa148-62d1-472c-b26d-4c1cfdcaa997",
	"adls_endpoint" : "https://dlstestidp001.dfs.core.windows.net",
	"adls_root_dir" : "idp-datalake",
	"adls_dest_dir" : "UKG",
	"input_dir_path" : "<path>/*"
	}

	latest_file = get_latest_file(params["input_dir_path"])

	if len(latest_file) > 0 :
		
		upload_to_adls(params,latest_file[0])
		remove_all_files(params["input_dir_path"])

	else:
		print("No files present for uploading to Adls at time : {}".format(datetime.now()))
		exit(0)

