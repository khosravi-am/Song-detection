import boto3
import requests
import logging
import pika
from botocore.exceptions import ClientError
import psycopg2
import conf


logging.basicConfig(level=logging.INFO)

def connect():
    config = conf.load_config()
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def rabbit_connect():
    try:
        config = conf.load_rab_conf()
        connection = pika.BlockingConnection(pika.URLParameters(**config))
        channel = connection.channel()
        channel.queue_declare(queue='request_ID')
        return connection, channel
    except Exception as exc:
        print("rabbit connect err: ",exc)
        logging.info(exc)


s3 = boto3.client('s3')
db_connection=connect()
rabbit_conn, rabbit_channel = rabbit_connect()
url = "https://shazam-api-free.p.rapidapi.com/shazam/recognize/"
header = conf.load_config(section='shazam')
spotify_header = conf.load_config(section='spotify')
print("header: \n",header)

def store_songID(id,song_id):
    query = """update requests set songid = %s where id = %s ;"""
    print('in store')
    res = None
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            print('in with')
            cur.execute(query, (song_id,id,))
            print('execute')
            # get the generated id back
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return res
    
def request_failed(id,status):
    query = """update requests set status = %s
               where id = %s;"""

    res = None
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (status,id,))
            # get the generated id back
            
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return res


def get_name(id):
    query = """select filename from requests where id = %s ;"""

    filename = ''
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (id,))
            # get the generated id back
            rows = cur.fetchone()
            if rows:
                filename = rows[0]
            # commit the changes to the database
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        print('filename: ',filename)
        return filename


def getfile(filename,bucket):
        """
        Gets the object.

        :return: The object data in bytes.
        """
        try:
            body = s3.get_object(Bucket=bucket, Key=filename)["Body"]
            print('content-type: ',s3.get_object(Bucket=bucket, Key=filename)["ContentType"])
            logging.info(
                "Got object '%s' from bucket '%s'.",
                filename,
                bucket,
            )
        except ClientError:
            logging.exception(
                "Couldn't get object '%s' from bucket '%s'.",
                filename,
                bucket,
            )
            raise
        else:
            return body
        
def get_songid(filename):
    url = "https://spotify23.p.rapidapi.com/search/"

    querystring = {"q": filename ,"type":"tracks","offset":"0","limit":"1","numberOfTopResults":"1"}
    print('query: ',querystring)
    response = requests.get(url, headers=spotify_header, params=querystring)
    print('result: \n',response.json())
    return response.json()['tracks']['items'][0]['data']['id']
    # print(response.json())

def callback(ch, method, properties, body):
        id = str(body).replace('b','').replace("'","")        
        print('id: ',id)
        if (id):
            try:
                filename = get_name(id)
                file = getfile(filename,'khosro-songs')
                files = file.read()   
                payload = {'upload_file': (str(filename), files, 'audio/mpeg')}
            # print('file:',files)
                response = requests.post(url, files=payload, headers= header)
                if response.status_code == 200 and 'track' in response.json():
                    spotify_name = response.json()['track']['title']
                    print('file name: ',spotify_name)
                    song_id = get_songid(spotify_name)
                    store_songID(id,song_id)
                    request_failed(id,'ready')
                    print("massage: id-> \n",song_id)
                else :
                    # return message
                    request_failed(id,'failure')
                    print('error: ',response.json())
            except Exception as e:
                print('callback err!: ',e)
            finally:
                print('end')



def main():
    rabbit_channel.basic_consume(queue='request_ID', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    rabbit_channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            exit(0)
        except SystemExit:
            exit(0)