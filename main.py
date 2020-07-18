import pickle
import os
import io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from tabulate import tabulate
from googleapiclient.http import MediaIoBaseDownload
import sys
from hurry.filesize import size, verbose
from apiclient import errors


class Drive():
    """Drive url"""
    # Functions:
    # get_gdrive_service()
    # downloadFile(file_id, path=None)
    # getFileObject(filename)
    # getFileObjectForFolder(filename, path)
    # list_files(query_string, path=None):
    # get_size_format(file_size):
    # parseID(url):
    # getMetaData( fileId):
    # isFolder(url):
    # download():

    def __init__(self, url,
                 initial_path=None, verbose=False):
        # Scope of getting the full access from google drive of a user
        self.SCOPES = ['https://www.googleapis.com/auth/drive']
        self.service = self.get_gdrive_service()
        self.URL = url
        self.verbose = verbose
        if initial_path is None:
            self.initial_path = os.path.join(os.getcwd(), "downloads")
            if not os.path.exists(self.initial_path):
                os.mkdir(self.initial_path)
        else:
            self.initial_path = initial_path

    def get_gdrive_service(self):
        """Gets the service object of google drive"""
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
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
        # return Google Drive API service
        return build('drive', 'v3', credentials=creds)

    def downloadFile(self, file_id, initial_path=None):
        """Function to download the google drive files"""

        fileName = self.getMetaData(file_id)['name']
        # if self.verbose:
        if initial_path is None:
            long_name = os.path.join(
                self.initial_path, initial_path or '', fileName)
            if os.path.exists(long_name):
                return
            # get the file object in write binary mode
            fileObj = self.getFileObject(fileName)
        else:
            # get the folder object in write binary mode
            print("recieved folder", initial_path, fileName)
            fileObj = self.getFileObjectForFolder(fileName, initial_path)
        # call the service api for downloading the media
        request = self.service.files().get_media(fileId=file_id)
        # request the media downloader
        downloader = MediaIoBaseDownload(fileObj, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            # Track the downloading progress
            print(
                "[*]Download Progress {0}% - {1}.".format(str(int(status.progress())*100), fileName))

    def getFileObject(self, filename):
        """Open the destination file in write mode"""
        return open(os.path.join(self.initial_path, filename), 'wb')

    def getFileObjectForFolder(self, filename, path):
        """Open the destination folder with an existing initial path in write mode"""
        return open(os.path.join(self.initial_path, path, filename), 'wb')

    def list_files(self, query_string, path=None, nextPageToken=None):
        """List the google drive files as returned by Google Drive API. 
        If verbose: then prints in tabular format."""

        if nextPageToken:
            items = self.service.files().list(
                q=query_string,
                pageToken=nextPageToken,
                pageSize=1000, fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
        else:
            items = self.service.files().list(
                q=query_string,
                pageSize=1000, fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
        # get the results

        nextPageToken=items.get('nextPageToken')
        items = items.get('files', [])
        if not items:
            # empty drive
            print(f'No files found in query_string "{query_string}".')
        else:
            rows = []
            for item in items:
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    folder = os.path.join(os.getcwd(), item['name'])
                    if not os.path.isdir(folder):
                        os.mkdir(folder)
                    self.list_files(query_string="'{0}' in parents".format(
                        item['id']), path=folder)
                else:
                    # get the File ID
                    id = item["id"]
                    if path is None:
                        self.downloadFile(id)
                    else:
                        self.downloadFile(id, path)
                    if self.verbose:
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
                        rows.append((id, name, parents, size,
                                     mime_type, modified_time))

            if self.verbose:
                print("Files:")
                # convert to a human readable table
                table = tabulate(
                    rows, headers=["ID", "Name", "Parents", "Size", "Type", "Modified Time"])
                # print the table
                print("table", table)
            
            if nextPageToken:
                print(f'Fetching next set of results".')
                folderId = self.parseID(self.URL)
                q = "'{0}' in parents".format(folderId)
                self.list_files(q,nextPageToken=nextPageToken)

    def get_size_format(self, file_size):
        """Returns the file sizes of given file"""
        return size(file_size, system=verbose)

    def parseID(self, url):
        """Retrieve the ID from given url"""
        return url.split('/')[-1].split('?')[0]

    def getMetaData(self, fileId):
        """Retrieve the files attributes"""
        try:
            return self.service.files().get(fileId=fileId).execute()
        except errors.HttpError as exception:
            raise errors.HttpError(f"An error occurered {exception}")

    def isFolder(self, url):
        """Function to check if the url is a folder"""
        folderId = self.parseID(url)
        try:
            file = self.getMetaData(folderId)
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                return True
            else:
                return False
        except Exception as e:
            pass

    def download(self):
        """Main entry point """
        if self.isFolder(self.URL):
            # Call the Drive v3 API
            folderId = self.parseID(self.URL)
            q = "'{0}' in parents".format(folderId)
            # list all files & folders
            self.list_files(q)
        else:
            raise ValueError("Please give the url of a folder")


def downloadFromGDrive(url, path=None):
    drive = Drive(url=str(url), initial_path=path)
    drive.download()


if __name__ == "__main__":
    FOLDER_URL="https://drive.google.com/drive/folders/1-cmUKkdiDwRz2otQfmGJf-NhDMAWqM1S?usp=sharing"
    downloadFromGDrive(FOLDER_URL)
