import pickle
import os, io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from tabulate import tabulate
from googleapiclient.http import MediaIoBaseDownload
from decouple import config
from hurry.filesize import size, verbose


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']


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

def downloadFile(file_id):
    global service;
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download {0}.".format(str(int(status.progress())*100)))
        # print "Download %d%%." % int(status.progress() * 100)

def list_files(items):
    """given items returned by Google Drive API, prints them in a tabular way"""
    if not items:
        # empty drive
        print('No files found.')
    else:
        rows = []
        for item in items:
            # input(">")
            # get the File ID
            id = item["id"]
            downloadFile(id)
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

def get_size_format(file_size):
    return size(file_size, system=verbose)

def parseID(url):
    return url.split('/')[-1].split('?')[0]

def checkFolderURL(service, url):
    folderId = parseID(url)
    try:
        file = service.files().get(fileId=folderId).execute()
        if file['mimeType'] == 'application/vnd.google-apps.folder':
            return True
        else:
            return False
    except Exception as e:
        pass

def main(url):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 5 files the user has access to.
    """
    global service;
    service = get_gdrive_service()
    if checkFolderURL(service, url):
        # Call the Drive v3 API
        folderId = parseID(url)
        q = "'{0}' in parents".format(folderId)
        results = service.files().list(
            q=q,
            pageSize=10, fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
        # get the results
        items = results.get('files', [])
        # list all 20 files & folders
        list_files(items)
    else:
        print("Please give the url of a folder")


if __name__ == "__main__":
    global service;
    url = config('FOLDER_URL', cast=str)
    main(url)
