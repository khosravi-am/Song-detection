from time import sleep 
import requests
from flask import Flask
from flask_mail import Mail, Message
import logging
from botocore.exceptions import ClientError
import psycopg2
import conf

logging.basicConfig(level=logging.INFO)
DELAY = 30

def connect():
    config = conf.load_config()
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print('db connect error: ',error)



db_connection=connect()
spotify_header = conf.load_config(section='spotify')
config = conf.load_config(section='mail')
app = Flask(__name__)
app.config['MAIL_SERVER'] = config['mail_server']
app.config['MAIL_PORT'] = config['mail_port']
app.config['MAIL_USERNAME'] = config['mail_username']
app.config['MAIL_PASSWORD'] = config['mail_password']
app.config['MAIL_USE_TLS'] = config['mail_use_tls']
app.config['MAIL_USE_SSL'] = False


def sendmail(id,email,message):

    with app.app_context():
        try:
            mail = Mail(app)
            msg = Message(
              'Mailing with khosro',
              sender =  ("khosro", 'info@autlib.ir'),
              recipients = [email,])
            print('msg is ready')
            msg.body = message
            print('body ready')
            mail.send(msg)
            print( "Message sent!")
            request_failed(id,'done')
        except Exception as e:
            print('send email error!: ',e)
            request_failed(id,'failure')



def request_failed(id,status):
    query = """update requests set status = %s
               where id = %s;"""

    res = None
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (status,id,))
            # get the generated id back
            # commit the changes to the database
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("set request status to db err!: ",error)
    finally:
        return res






def check_db_readys():
    query = """select id,email,songid from requests where status = 'ready' ;"""

    ready_list = []
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query,)
            # get the generated id back
            ready_list = cur.fetchall()
            # print("rows: ",ready_list)
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("db get readys err! :", error)
    finally:
        # print('filename: ',filename)
        return ready_list



def get_recom(song_id,limit = 5):
    url = "https://spotify23.p.rapidapi.com/recommendations/"
    songs = ''
    
    querystring = {"limit":str(limit),"seed_tracks":song_id}
    try:
        response = requests.get(url, headers=spotify_header, params=querystring)
        print('response: ',response.json())
        if response.status_code == 200 :
            for i in range(int(limit)):
                name = str(response.json()['tracks'][i]['name'])
                preview_url = str(response.json()['tracks'][i]['preview_url'])
                external_urls = str(response.json()['tracks'][i]['external_urls'])
                songs = songs + '(name: '+ name + ', preview_url: ' + preview_url + ', external_urls: ' + external_urls + ')\n'
    except Exception as e:
        print('get recom err!: ',e)
    finally:
        return songs
    

while (True):
    try:
        emails = check_db_readys()
        print('emails: ', emails)
        if emails:
            for id,email,songid in emails:
                print("id: ",id,"email: ",email,"sid: ",songid)
                result = get_recom(song_id=songid)
                
                # print('result: \n',result)
                if (result):
                    print("songs: \n",result)
                    sendmail(id,email,result)
            
        
    except Exception as e:
        print('err!: ',e)
    finally:
        sleep(DELAY)