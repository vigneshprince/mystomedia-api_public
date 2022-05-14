from fastapi import FastAPI,BackgroundTasks,Request
import os
from fastapi.middleware.cors import CORSMiddleware
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import requests
import sqlite3
from urllib.parse import unquote
import uuid
import bs4
import urllib
from fastapi import BackgroundTasks
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request as GD_Request
import pickle
import os
from googleapiclient.discovery import MediaFileUpload
from apscheduler.schedulers.background import BackgroundScheduler
import re
from imdb import Cinemagoer
from fastapi.staticfiles import StaticFiles
from asyncio import Semaphore, gather, run, wait_for
import aiofiles
from aiohttp.client import ClientSession

ia = Cinemagoer()

GD_tamil_ID = '14YsLxnLG4h4rpCBcHv2sE5maicG59nN9'
GD_FOLDER_ID = '0AImUfJK1GLwMUk9PVA'
GD_YT= '13MWP50ZncSnyQLljhQCLBfRl516lpB-E'

SCOPES = ['https://www.googleapis.com/auth/drive']
creds = None
if os.path.exists('gdrive_cred/token.pickle'):
    with open('gdrive_cred/token.pickle', 'rb') as token:
        creds = pickle.load(token)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(GD_Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'gdrive_cred/credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('gdrive_cred/token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('drive', 'v3', credentials=creds)


opener = urllib.request.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0')]
urllib.request.install_opener(opener)

conn = sqlite3.connect('data.sqlite')
cursor = conn.cursor()

app = FastAPI()
app.mount("/imgs", StaticFiles(directory="imgs"), name="imgs")

origins = [
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

jobs={}



async def download_imgs(links):
    tasks = []
    sem = Semaphore(100)

    async with ClientSession() as sess:
        for link in links:
            # Mock a different file name each iteration
            tasks.append(
                # Wait max 5 seconds for each download
                 download_one_img(link[0], sess, sem, link[1]),
            )

        return await gather(*tasks)


async def download_one_img(url, sess, sem, dest_file):
    async with sem:
        async with sess.get(url) as res:
            content = await res.read()

        # Check everything went well
        if res.status != 200:
            return

        async with aiofiles.open(dest_file, "+wb") as f:
            await f.write(content)

def ping():
    requests.get('https://mystomedia-api.herokuapp.com/')
    print('pinged')

scheduler = BackgroundScheduler()
scheduler.add_job(ping, trigger="interval", minutes=10)
scheduler.start()

@app.post("/gettamilmoviename")
async def gettamilmoviename(info : Request):
    req_info = await info.json()
    cursor.execute(f"SELECT distinct level1 FROM data where level1 like '%{req_info['name']}%'")
    return [dict(zip([column[0] for column in cursor.description], row))
             for row in cursor.fetchall()]

@app.post("/gettamiltrackfromsearch")
async def gettamilmoviename(info : Request):
    req_info = await info.json()
    cursor.execute(f"SELECT level3_link,level3 FROM data where level3 like '%{req_info['name']}%'")
    return [dict(zip([column[0] for column in cursor.description], row))
             for row in cursor.fetchall()]

@app.post("/gettamiltrackname")
async def gettamiltrackname(info : Request):
    req_info = await info.json()
    print(req_info)
    cursor.execute(f"SELECT level3_link,level3 FROM data where level1 = '{req_info['name']}'")

    return [dict(zip([column[0] for column in cursor.description], row))
             for row in cursor.fetchall()]

def download(uid,link,name,parent):
    fname=unquote(link)
    flag=0
    with open(os.path.basename(fname), 'wb') as f:
        response = requests.get(link, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
            jobs[uid]={'key':uid,'name':name,'status':'failed','done': 0, 'total': 0}
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                try:
                    downloaded += len(data)
                    f.write(data)
                    jobs[uid]={'key':uid,'status':'Downloading','done': downloaded/(1024*1024), 'total': total/(1024*1024),'name':name,'percent':int((downloaded/total)*100)}
                except:
                    jobs[uid]={'key':uid,'status':'Failed','done': downloaded/(1024*1024), 'total': total/(1024*1024),'name':name,'percent':int((downloaded/total)*100)}
                    flag=1
                    break
    if flag==0:
        fname=os.path.basename(fname)
        fsize=os.path.getsize(fname)/(1024*1024)
        media = MediaFileUpload(fname,
                            mimetype='*/*',
                            resumable=True,
                            chunksize=52428800
                            )
        file = service.files().create(supportsTeamDrives=True,
                                    body={
                                        'name': fname,
                                        'parents': [parent]
                                    },
                                    media_body=media,
                                    fields='id'
                                    )
        media.stream()  # this line doesn't exist in the guide... ###
        response = None
        while response is None:
            status, response = file.next_chunk()
            if status:
                done=fsize*status.progress()
                jobs[uid]={'key':uid,'status':'Uploading','done': done, 'total': fsize,'name':name,'percent':int((done/fsize)*100)}
        jobs[uid]={'key':uid,'status':'Finished','done': fsize, 'total': fsize,'name':name,'percent':100}
        media.stream().close()
        os.remove(fname)
        
def download_Tamilyogi(uid,link,name,parent):
    flag=0
    print(name)
    with open(os.path.basename(name), 'wb') as f:
        response = requests.get(link, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
            jobs[uid]={'key':uid,'name':name,'status':'failed','done': 0, 'total': 0}
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                try:
                    downloaded += len(data)
                    f.write(data)
                    jobs[uid]={'key':uid,'status':'Downloading','done': downloaded/(1024*1024), 'total': total/(1024*1024),'name':name,'percent':int((downloaded/total)*100)}
                except:
                    jobs[uid]={'key':uid,'status':'Failed','done': downloaded/(1024*1024), 'total': total/(1024*1024),'name':name,'percent':int((downloaded/total)*100)}
                    flag=1
                    break
    if flag==0:
        fsize=os.path.getsize(name)/(1024*1024)
        media = MediaFileUpload(name,
                            mimetype='*/*',
                            resumable=True,
                            chunksize=52428800
                            )
        file = service.files().create(supportsTeamDrives=True,
                                    body={
                                        'name': name,
                                        'parents': [parent]
                                    },
                                    media_body=media,
                                    fields='id'
                                    )
        media.stream()  # this line doesn't exist in the guide... ###
        response = None
        while response is None:
            status, response = file.next_chunk()
            if status:
                done=fsize*status.progress()
                jobs[uid]={'key':uid,'status':'Uploading','done': done, 'total': fsize,'name':name,'percent':int((done/fsize)*100)}
        jobs[uid]={'key':uid,'status':'Finished','done': fsize, 'total': fsize,'name':name,'percent':100}
        media.stream().close()
        os.remove(name)
        


@app.post("/downloadtamilvideo")
async def downloadtamilvideo(info : Request, background_tasks: BackgroundTasks):
    info = await info.json()
    link=info['link']
    mainsoup = bs4.BeautifulSoup(urllib.request.urlopen(link), 'html.parser')
    for atag in mainsoup.find_all('a'):
        if('Server' in atag.string):  
            link=atag.get('href')

    uid=str(uuid.uuid4())
    jobs[uid] = {'key':uid,'status': 'started','total':0,'done':0,'name':info['name'],'percent':0}
    background_tasks.add_task(download, uid, link,info['name'],GD_tamil_ID)
    return jobs[uid]

@app.post("/jobstatus")
async def statustamilvideo(info : Request):
    info=await info.json()
    li=[]
    for x in info['jobid']:
        if x in jobs:
            li.append(jobs[x])
    return li


@app.post('/gdfilesearch')
async def filesearch(info : Request):
    gdSearch=await info.json()
    q = "mimeType contains 'video/' and trashed=false and fullText contains '{}'".format(
        gdSearch['name'])

    query = service.files().list(
        corpora="allDrives",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
        fields="files/name,files/id,files/parents,files/mimeType,files/size",
        q=q
    ).execute()
    return sorted(query['files'], key=lambda x: int(x['size']), reverse=True)


@app.post('/gdfoldersearch')
async def foldersearch(info : Request):
    gdSearch=await info.json()
    q = "mimeType='application/vnd.google-apps.folder' and trashed=false and fullText contains '{}'".format(
        gdSearch['name'])

    query = service.files().list(
        corpora="allDrives",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
        fields="files/name,files/id,files/parents,files/mimeType",
        q=q
    ).execute()

    return query['files']

def downloadGdrive(uid,newfile,id,name,path):
    try:
        service.files().copy(
            fileId=id, supportsAllDrives=True, body=newfile).execute()
        jobs[uid]={'key':uid,'status':'Finished','name':name,'parent_path':path}
    except:
        jobs[uid]={'key':uid,'status':'Failed','name':name,'parent_path':path}


@app.post('/gdfiletransfer')
async def filetranfer(info : Request, background_tasks: BackgroundTasks):
        gdtransfer=await info.json()
        newfile = {'name': gdtransfer['name'], 'parents': [GD_FOLDER_ID ]}
        uid=str(uuid.uuid4())
        jobs[uid] = {'key':uid,'status': 'Transferring','name':gdtransfer['name']}
        background_tasks.add_task(downloadGdrive, uid, newfile,gdtransfer['id'],gdtransfer['name'],'/')
        return jobs[uid]

def downloadMultiple(data,folder):
    for i in data:
        downloadGdrive(i['uid'],i['newfile'],i['id'],i['name'],f'/{folder}/')


@app.post('/gdfoldertransfer')
async def foldercreate(info : Request, background_tasks: BackgroundTasks):
    gdSearch=await info.json()
    file_metadata = {
        'name': gdSearch['folder'],
        'parents': [GD_FOLDER_ID],
        'mimeType': 'application/vnd.google-apps.folder'
    }

    folder_info = service.files().create(
        body=file_metadata, supportsAllDrives=True, fields='id').execute()

    ret_list=[]
    data=[]
    for i in gdSearch['files']:
        newfile = {'name': i['name'], 'parents': [folder_info['id']]}
        uid=str(uuid.uuid4())
        jobs[uid] = {'key':uid,'status': 'Transferring','name':i['name']}
        ret_list.append(jobs[uid])
        data.append({'uid':uid,'newfile':newfile,'id':i['id'],'name':i['name']})
    background_tasks.add_task(downloadMultiple, data,gdSearch['folder'])
    return ret_list



@app.post('/gdchildrensearch')
async def childrensearch(info : Request):
    gdSearch=await info.json()
    q = f"'{gdSearch['name']}' in parents and trashed=false"

    query = service.files().list(
        corpora="allDrives",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        pageSize=1000,
        fields="files/name,files/id,files/parents,files/mimeType,files/size",
        q=q
    ).execute()

    filelist = list(filter(
        lambda x: x['mimeType'] != 'application/vnd.google-apps.folder', query['files']))
    folderlist = list(filter(
        lambda x: x['mimeType'] == 'application/vnd.google-apps.folder', query['files']))
    folder_size=sum(map(lambda x: int(x['size']), filelist))
    return {'files': sorted(filelist, key=lambda x: x['name']), 'folders': folderlist,'folder_size':folder_size}


@app.get('/tamiyogilatest')
async def tamiyogilatest():
    mainsoup = bs4.BeautifulSoup(requests.get('http://tamilyogi.best/category/tamilyogi-bluray-movies').content, 'html.parser')
    data=[]
    img_list=[]
    for li in mainsoup.findAll('li'):
        div=li.find('div',{'class':'postcontent'})
        cover=li.find('img')
        if div and cover:
            match=re.match(r'(.*)\s*\((\d\d\d\d)\)', div.find('a')['title'])
            img_list.append([cover['src'],f"./imgs/{match.group(1)+match.group(2)}.jpg"])
            data.append({'link':div.find('a')['href'],'name':match.group(1)+match.group(2),'fullname':div.find('a')['title']})
    await download_imgs(img_list)
    return data

@app.post('/tamilyogisearch')
async def tamilyogisearch(info : Request):
    info=await info.json()
    mainsoup = bs4.BeautifulSoup(requests.get(f'http://tamilyogi.best/?s={info["name"]}').content, 'html.parser')
    data=[]
    img_list=[]
    for li in mainsoup.findAll('li'):
        div=li.find('div',{'class':'postcontent'})
        cover=li.find('img')
        if div and cover:
            match=re.match(r'(.*)\s*\((\d\d\d\d)\)', div.find('a')['title'])
            img_list.append([cover['src'],f"./imgs/{match.group(1)+match.group(2)}.jpg"])
            data.append({'link':div.find('a')['href'],'name':match.group(1)+match.group(2),'fullname':div.find('a')['title']})
    await download_imgs(img_list)
    return data

@app.post('/tamilyogiselectprint')
async def tamilyogiselectprint(info : Request):
    info=await info.json()
    iframe = bs4.BeautifulSoup(requests.get(info['link']).content, 'html.parser')
    iframe=iframe.findAll('iframe')[0]['src']
    video_page = bs4.BeautifulSoup(requests.get(iframe).content, 'html.parser')
    script=video_page.find("script",text=re.compile(r'.*v.mp4.*')).contents[0]
    return dict(re.findall(r'(http[^}]*v.mp4)",label:"(\d\d\d\w)', script))


@app.post('/tamilyogidownload')
async def tamilyogidownload(info : Request, background_tasks: BackgroundTasks):
    info=await info.json()
    uid=str(uuid.uuid4())
    jobs[uid] = {'key':uid,'status': 'started','total':0,'done':0,'name':info['name'],'percent':0}
    background_tasks.add_task(download_Tamilyogi, uid, info['link'],info['name'],GD_FOLDER_ID)
    return jobs[uid]


@app.post('/imdbmovie')
async def imdbmovie(info : Request):
    info=await info.json()
    try:
        movies = ia.search_movie(info['name'])
        movie = ia.get_movie(movies[0].movieID)
        cast=[i['name'] for i in movie['cast'][:10]]
        data={'cast':cast,'genres':movie.get('genres'),'runtimes':movie.get('runtimes'),'rating':movie.get('rating'),'votes':movie.get('votes'),'cover url':movie.get('cover url'),'director':movie.get('director')[0]['name'],'plot':movie.get('plot')}
        if movie.get('certificates'):
            certs=[s.split(':')[1] for s in movie.get('certificates') if s.startswith('India') ]
            if len(certs)>0:
                data['certificates']=certs[0]
            else:
                data['certificates']='None'
        else:
            data['certificates']='None'
        return data
    except Exception as e:
        print(e)
        return None



    
