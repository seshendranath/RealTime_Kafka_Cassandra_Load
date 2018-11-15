import boto3

s3Client = boto3.client('s3')
s3Resource = boto3.resource('s3')

bucket = 'indeed-data'


def get_latest_file(prefix):
    d = {}
    kwargs = {'Bucket': bucket, 'Prefix': prefix}

    while True:
        resp = s3Client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if 'offsets' in key and '$folder$' not in key:
                tbl = key.split('/')[5]
                d[tbl] = key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return list(d.values())


def s3_read(key):
    body = s3Resource.Object(bucket, key).get()['Body']
    return body.read().decode("utf-8")


keys = get_latest_file('datalake/v1/stage/binlog_events/checkpoint')

min_offsets = s3_read(keys[0])

for k in keys:
    offsets = s3_read(k)
    if offsets < min_offsets:
        min_offsets = offsets


print('--offsetString="' + min_offsets.split('\n')[-1].replace('"', '\\"') + '"')

# --offsetString="{\"maxwell\":{\"92\":-1,\"83\":-1,\"23\":-1,\"95\":-1,\"77\":-1,\"86\":-1,\"50\":-1,\"59\":-1,\"41\":-1,\"32\":-1,\"68\":-1,\"53\":-1,\"62\":-1,\"35\":-1,\"44\":-1,\"8\":-1,\"17\":-1,\"26\":-1,\"80\":-1,\"89\":-1,\"98\":-1,\"71\":-1,\"11\":-1,\"74\":-1,\"56\":-1,\"38\":-1,\"29\":-1,\"47\":-1,\"20\":-1,\"2\":-1,\"65\":-1,\"5\":-1,\"14\":-1,\"46\":-1,\"82\":-1,\"91\":-1,\"55\":-1,\"64\":-1,\"73\":-1,\"58\":-1,\"67\":-1,\"85\":-1,\"94\":-1,\"40\":-1,\"49\":-1,\"4\":-1,\"13\":-1,\"22\":-1,\"31\":-1,\"76\":-1,\"16\":-1,\"97\":-1,\"7\":-1,\"79\":-1,\"88\":-1,\"70\":-2,\"43\":-1,\"52\":-1,\"25\":-1,\"34\":-1,\"61\":-1,\"10\":-2,\"37\":-1,\"1\":-1,\"28\":-1,\"19\":-1,\"60\":-1,\"87\":-1,\"96\":-1,\"69\":-1,\"78\":-1,\"99\":-1,\"63\":-1,\"90\":-1,\"45\":-1,\"54\":-1,\"72\":-1,\"81\":-1,\"27\":-1,\"36\":-1,\"9\":-1,\"18\":-1,\"48\":-1,\"21\":-1,\"57\":-1,\"12\":-1,\"3\":-1,\"84\":-1,\"93\":-1,\"75\":-1,\"30\":-1,\"39\":-1,\"66\":-1,\"15\":-1,\"42\":-1,\"51\":-1,\"33\":-1,\"24\":-1,\"6\":-1,\"0\":-1}}"
