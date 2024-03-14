# Song-detection
In this project, song detection and suggestion service has been implemented, which includes 3 main services. After receiving the request with the destination email and the audio file, the first service saves the audio file in the aws object storage. Then the request information is transferred to the second service. The second service identifies the song with Spotify and Shazam tools and then sends the relevant information to the third service. Finally, the third service sends the suggested songs to the user's email using Mailgun or other mail services. This project is programmed with Python language and FLASK tool and uses Postgresql as database.

# Usage
you must first enter your object storage, RabbitMQ Postgresql, Spotify, Shazam, and email server credentials in db.ini.
# install requirments.txt

# run: 
```
python3 main.py 
python3 songer.py 
python3 mail.py
```
