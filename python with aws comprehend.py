import boto3,os,json,sys
client = boto3.client('s3')
s3 = boto3.resource('s3')
comprehend=boto3.client('comprehend')
my_bucket = s3.Bucket('crewler-data')
for file in my_bucket.objects.filter(Prefix='folder_name'):
    name=file.key
    title=os.path.splitext(name)[0]
    entities=title+".Entities.json"
    keyphrases=title+".Keyphrases.json"
    body = file.get()['Body'].read()
    content=json.loads(body)
    data=content['content']
    Keypharse=json.dumps(comprehend.detect_key_phrases(Text=data, LanguageCode='en'), sort_keys=True, indent=4)
    Entities=json.dumps(comprehend.detect_entities(Text=data, LanguageCode='en'), sort_keys=True, indent=4)
    client.put_object(Bucket='crewler-data', Key=entities, Body=Entities)
    client.put_object(Bucket='crewler-data', Key=keyphrases, Body=Keypharse)

