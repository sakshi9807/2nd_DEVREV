import psycopg2
import boto3


conn = psycopg2.connect(
    host='sak',
    dbname='bucket',
    user='Saks',
    password='Sak3',
    port='3000'
)


cur = conn.cursor()


cur.execute('CREATE TABLE IF NOT EXISTS staging_songs (\
             num_songs INT,\
             artist_id VARCHAR(255),\
             artist_latitude FLOAT,\
             artist_longitude FLOAT,\
             artist_location VARCHAR(255),\
             artist_name VARCHAR(255),\
             song_id VARCHAR(255),\
             title VARCHAR(255),\
             duration FLOAT,\
             year INT\
             );')


conn.commit()


s3 = boto3.client('s3')


s3.download_file('your-bucket-name', 'your-song-data-key.json', 'local-file-path.json')


cur.execute(f"COPY staging_songs FROM 'local-file-path.json' \
             credentials 'aws_iam_role=your-iam-role-arn' \
             region 'your-aws-region' \
             json 'auto'\
             ")


conn.commit()


cur.close()
conn.close()