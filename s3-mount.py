# copy from iam users..
access_key = '<<your key>>'
secret_key =  '<<your secret key>>'
encoded_secret_key = secret_key.replace("/", "%2F")
# ****************
aws_bucket_name = "<<your-bucket-name>>"
# mount name = aws , you can access gks-bucket content as /mnt/aws/movies/movies.csv
mount_name = "aws"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))




############################
#####

# to unmount 

dbutils.fs.unmount('/mnt/aws')
