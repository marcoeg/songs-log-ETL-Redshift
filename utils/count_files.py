import boto3
import json
import configparser
import time

config = configparser.ConfigParser()
config.read_file(open('./dwh-create.cfg'))

KEY                    = config.get('AWS', 'KEY')
SECRET                 = config.get('AWS', 'SECRET')


s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

# sum of file sizes
fnum, tsize = 0, 0
sampleDbBucket =  s3.Bucket("udacity-dend")
#for obj in sampleDbBucket.objects.filter(Prefix='song-data'):
for obj in sampleDbBucket.objects.filter(Prefix='song_data'):
    fnum += 1
    if fnum % 100 == 0:
    	print(fnum)
    tsize += obj.size

print ("Song Files: {}".format(fnum))
print ("Total size, MB: {}".format(tsize/1024/1024))
