# streamlit_app.py

import streamlit as st
import s3fs
import os
import pandas as pd
import time
import requests
import io
from multiprocessing.pool import ThreadPool 
from urllib.parse import urlparse, unquote


upload_tab, table_tab = st.tabs(["Upload", "File Status"])

# Create connection object.
# `anon=False` means not anonymous, i.e. it uses access keys to pull data.
fs = s3fs.S3FileSystem(anon=False)
s3 = s3fs.S3FileSystem(anon=False, asynchronous=True)
bucket_name = os.environ["AWS_S3_BUCKET_NAME"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"] 

def get_size_in_mbs(file_bytes):
    byte = int(file_bytes)
    mb = round(byte/1048576, 2)
    mb_formatted = "{} MB".format(mb)
    return mb_formatted

def get_substring(my_string, delim):
    return my_string.split(delim,1)[1]

def get_casename_from_url(file_url):
    filename = get_file_name_from_url(file_url)
    return filename.split('.')[0]

def get_file_name_from_url(file_url):
    filename = urlparse(file_url)
    unquoted_filename = unquote(filename.path)
    return unquoted_filename.split('/')[1]

def upload_api(file_url):
    url =os.environ['URL_UAT']
    api_key_value = os.environ['API_UAT_KEY']
    headers_obj = {'x-api-key' : api_key_value}

    formdata={}
    formdata['FilingParty'] = 'TestPartyA'
    formdata['Email'] = 'filing@tadchievlaw.com'
    formdata['FileNumber'] = get_casename_from_url(file_url)

    file_data = get_file_data_from_url(file_url)
    files=[('Files',(get_file_name_from_url(file_url),file_data,'application/pdf'))]

    try:
        r = requests.post(url,files=files, headers = headers_obj, timeout=100, data=formdata)
        if r.status_code == 200:
            st.success('done')
            st.write(r.text)
            fs.rm(get_file_name_from_url(file_url))
        else:
            st.error('statusCode: '+ str(r.status_code) + '\n' + r.text, icon="🚨")
    except Exception as e:
       st.exception(e)

def get_file_data_from_url(file_url):
    try:
        r = requests.get(file_url, allow_redirects=True)
        if r.status_code != 200:
            st.error("Unable to retrieve file data", icon="🚨")
            return None
        return io.BytesIO(r.content)
    except Exception as e:
        st.exception(e)


def write_to_s3(data):
    return fs.write_bytes(bucket_name +    '/' + data['filename'], data['bytes_data'])

# def write_to_s3(filename, bytes_data):
#     fs.write_bytes(bucket_name +    '/' + filename, bytes_data)

def get_url_from_S3(filename):
    return fs.sign(bucket_name + '/' + filename)

def get_url_and_call_api(filename):
    file_n = filename.split('/')[1]
    file_url = get_url_from_S3(file_n)
    print (file_url)
    upload_api(file_url)

with upload_tab:
    uploaded_files = st.file_uploader("Choose files to process", accept_multiple_files=True)
    data_list = []
    if uploaded_files is not None:
        with st.spinner('Wait for it...'):
            total_start = time.time()
            for uploaded_file in uploaded_files: 
                bytes_data = uploaded_file.read()
                data = {}
                data['filename'] = uploaded_file.name   
                data['bytes_data'] = bytes_data
                data_list.append(data)
                st.write("Queuing file: " + uploaded_file.name)

            with ThreadPool(2) as pool:
                start = time.time()
                pool.map(write_to_s3, data_list)
                pool.close()
                pool.join()
                end = time.time()
                st.write("s3 total upload time: " + str(end-start))

with table_tab:
    left_col, right_col = st.columns([1.3,1])
    filenames = fs.ls('cases-filling-s3-2023')

    with right_col:
        submit_btn = st.button("submit", key="submit_btn")
        if submit_btn:
            if filenames is None or len(filenames) == 0:
                st.error("No cases to submit, please upload files")
            else:
                with ThreadPool(3) as pool:
                    start = time.time()
                    pool.map(get_url_and_call_api, filenames)
                    pool.close()
                    pool.join()
                    end = time.time()
                    st.write("s3 total upload time: " + str(end-start))

    with left_col:
        del_btn = st.button("delete", key="del_btn")

        if del_btn:
            filenames = fs.ls('cases-filling-s3-2023')
            for file in filenames:
                fs.rm(file)

    data_list = []
    for file in filenames:
        file_info_dict = fs.info(file)
        file_size = file_info_dict['Size']
        file_size_mb = get_size_in_mbs(file_size)

        
        data_dict = dict()
        file_list = file.split('/')
        bucket_n = file_list[0]
        file_n = file_list[1]
        data_dict['bucket'] = bucket_n
        data_dict['filename'] = file_n
        data_dict['size'] = file_size_mb
        data_dict['status'] = 'uploaded'
        data_list.append(data_dict)
    
    # Create DataFrame from list of dic object
    df=pd.DataFrame(data_list)
    st.dataframe(df)




