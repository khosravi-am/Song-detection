from flask import Flask, flash, request, redirect, render_template
from werkzeug.utils import secure_filename
import boto3
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



UPLOAD_FOLDER = '/home/khosro/songDetect/uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif','m4a','mp3','mp4','flac','wav','aac','wma'}

db_connection=connect()
rabbit_conn, rabbit_channel = rabbit_connect()
storage = boto3.client('s3')

app = Flask(__name__,'/static')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000





def store_file(file, filename):
    
    try:
        storage.put_object(
             Bucket='khosro-songs',
             ACL='private',
             Body=file,
             Key=filename
         )
        
        print('send obj')
    except ClientError as e:
        print("last except")
        logging.error(e)


def get_status(id):
    query = """select status from requests where id = %s ;"""

    status = ''
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (id,))
            # get the generated id back
            rows = cur.fetchone()
            if rows:
                status = rows[0]
            print("rows: ",status)
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # print('filename: ',filename)
        return status





def db_store_email(email,status,filename):
    query = """INSERT INTO requests(email,status,filename) VALUES(%s,%s,%s) RETURNING id ;"""

    id = None
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (email,status,filename))
            # get the generated id back
            rows = cur.fetchone()
            if rows:
                id = rows[0]
            # commit the changes to the database
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('store email err!: ',error)
        db_connection.cursor().execute("ROLLBACK")

    finally:
        return id

def request_failed(id,status):
    query = """update requests set status = %s
               where id = %s;"""

    res = None
    try:
        with  db_connection.cursor() as cur:
            # execute the INSERT statement
            cur.execute(query, (status,id,))
            # get the generated id back
            rows = cur.fetchone()
            if rows:
                res = rows[0]
            # commit the changes to the database
            db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return res

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/',methods=['GET', 'POST'])
def main():

    if request.method == 'POST':
            # check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
            print('files: ',request.files)
            file = request.files['file']
            email = request.form.get('email')
            print("email: ", email)
            # If the user does not select a file, the browser submits an
            # empty file without a filename.
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            print('hello')
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                print('secure')
                store_file(file,filename)
                print('save')
                try:
                    message_Id=db_store_email(email,'pending',filename)
                    rabbit_channel.basic_publish(exchange='',routing_key='request_ID',body=str(message_Id))
                    return render_template("success.html", name=filename,Id=message_Id)
                except Exception as e:
                    request_failed(message_Id,'failure')
                    print("error: ",e)
                    return f'An error occurred!. Please try again in a few minutes!'
            else:
                return f'An error occurred!. Please try again in a few minutes!'
            
    return render_template("index.html")



@app.route('/<int:id>')
def show_result(id):
    # todo check the security of id
    status = get_status(id)
    if status == '' or status == 'failure':
        return f'An error occurred!. Please try again later!'
    if status == 'pending':
        return f'the status : {status} \n We are trying to find similar songs.'
    if status == 'ready': 
        return f'the status : {status} \n We are sending an email\n please wait.'
    if status == 'done':
        return f'the status : {status} \n the email has been sent!'


if __name__ == '__main__':
	app.run(debug=True)


