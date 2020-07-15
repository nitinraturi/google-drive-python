import pickle
import os, io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from tabulate import tabulate
from googleapiclient.http import MediaIoBaseDownload
from decouple import config
from hurry.filesize import size, verbose
from apiclient import errors


# Scope of getting the full access from google drive of a user
SCOPES = ['https://www.googleapis.com/auth/drive']

# Function for getting the service object of google drive
def get_gdrive_service():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # return Google Drive API service
    return build('drive', 'v3', credentials=creds)

# Function for download the google drive files
def downloadFile(file_id, path=None):
    global service;
    fileName = getMetaData(file_id)['name']
    if path is None:
        # get the file object in write binary mode
        fileObj = getFileObject(fileName)
    else:
        # get the file object in write binary mode
        fileObj = getFileObjectForFolder(fileName, path)
    # call the service api for downloading the media
    request = service.files().get_media(fileId=file_id)
    # request the media downloader
    downloader = MediaIoBaseDownload(fileObj, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        # Track the downloading progress
        print("Download Progress {0}.".format(str(int(status.progress())*100)))

# Function for getting the file object opened
def getFileObject(filename):
    return open(os.path.join(os.getcwd(), filename), 'wb')

# Function for getting the file object opened
def getFileObjectForFolder(filename, path):
    print(path+filename)
    return open(path+'/'+filename, 'wb')

# Function for the listing the google drive files
def list_files(query_string, path=None):
    """given items returned by Google Drive API, prints them in a tabular way"""
    global service;
    items = service.files().list(
        q=query_string,
        pageSize=10, fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
    # get the results
    items = items.get('files', [])
    if not items:
        # empty drive
        print('No files found.')
    else:
        rows = []
        for item in items:
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                if not os.path.isdir(os.path.join(os.getcwd(), item['name'])):
                    os.mkdir(os.path.join(os.getcwd(), item['name']))
                list_files(query_string="'{0}' in parents".format(item['id']), path=os.path.join(os.getcwd(), item['name']))
            else:
                # get the File ID
                id = item["id"]
                if path is None:
                    downloadFile(id)
                else:
                    downloadFile(id, path)
                # get the name of file
                name = item["name"]
                try:
                    # parent directory ID
                    parents = item["parents"]
                except:
                    # has no parrents
                    parents = "N/A"
                try:
                    # get the size in nice bytes format (KB, MB, etc.)
                    size = get_size_format(int(item["size"]))
                except:
                    # not a file, may be a folder
                    size = "N/A"
                # get the Google Drive type of file
                mime_type = item["mimeType"]
                # get last modified date time
                modified_time = item["modifiedTime"]
                # append everything to the list
                rows.append((id, name, parents, size, mime_type, modified_time))
        print("Files:")
        # convert to a human readable table
        table = tabulate(
            rows, headers=["ID", "Name", "Parents", "Size", "Type", "Modified Time"])
        # print the table
        print("table",table)

# Function for formatting the file sizes
def get_size_format(file_size):
    return size(file_size, system=verbose)

# Function for retrieving the ID from given url
def parseID(url):
    return url.split('/')[-1].split('?')[0]

# Function for retrieving the files attributes
def getMetaData(fileId):
    global service;
    try:
        return service.files().get(fileId=fileId).execute()
    except errors.HttpError as e:
        print("An error occurered {}".format(e))

# Function for check the url is of folder
def checkFolderURL(url):
    global service;
    folderId = parseID(url)
    try:
        file = getMetaData(folderId)
        if file['mimeType'] == 'application/vnd.google-apps.folder':
            return True
        else:
            return False
    except Exception as e:
        pass

# Main entry point
def main(url):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 5 files the user has access to.
    """
    global service;
    service = get_gdrive_service()
    if checkFolderURL(url):
        # Call the Drive v3 API
        folderId = parseID(url)
        q = "'{0}' in parents".format(folderId)
        # list all 20 files & folders
        list_files(q)
    else:
        print("Please give the url of a folder")


if __name__ == "__main__":
    global service;
    # Folder url from environment file
    url = config('FOLDER_URL', cast=str)
    main(url)