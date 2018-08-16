import os
import io
import requests
import boto3
import json
from PIL import Image
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.WARNING)

if os.environ.get('AWS_EXECUTION_ENV') is None:
    session = boto3.Session(profile_name='my-local-aws-profile')
else:
    session = boto3.Session()


# Builds url with editable seperated query options
def build_request_url():
    image_count = int(os.environ['IMAGE_COUNT'])
    orientation = os.environ['IMAGE_ORIENTATION']
    search_keywords = os.environ['SEARCH_KEYWORDS']
    request_url = '%srandom?client_id=%s&count=%s&orientation=%s&query=%s' % (
        os.environ['UNSPLASH_API_URL'],
        os.environ['UNSPLASH_CLIENT_ID'],
        image_count,
        orientation,
        search_keywords
    )
    return request_url


# Requests unsplash for random images.
def get_random_images(request_url):
    try:
        image_json = requests.get(request_url, timeout=120)
    except requests.exceptions.RequestException as e:
        logger.error('Failed to fetch json from api')
        return False
    return image_json.json()


# Transforms api images result to a simplified images object.
def filter_images(data):
    image_quality = 'full'
    s3_data = []
    for item in data:
        if not item['user']['first_name']:
            item['user']['first_name'] = ''
        if not item['user']['last_name']:
            item['user']['last_name'] = ''
        if not item['links']['html']:
            item['links']['html'] = ''
        image_object = {
            'image_url': item['urls'][image_quality],
            'thumbnail_url': item['urls']['thumb'],
            'meta_data': {
                'name': item['user']['first_name'] + ' ' + item['user']['last_name'],
                'profile': item['links']['html']
            }
        }
        s3_data.append(image_object)
    return s3_data


# Resizes an image.
def resize_image(image, width, height):
    image = Image.open(image)
    resized = image.resize((width, height))
    in_mem_file = io.BytesIO()
    resized.save(in_mem_file, 'JPEG')
    in_mem_file.seek(0)
    return in_mem_file


# Uploads an image to S3.
def upload_image_to_s3(s3_client, image_url, filename, is_thumbnail):
    try:
        response = requests.get(image_url)
    except requests.exceptions.RequestException as e:
        logger.error('Failed to download image file {}'.format(e))
        return False
    if (is_thumbnail):
        return s3_client.upload_fileobj(io.BytesIO(response.content), os.environ['S3_BUCKET'], os.environ['BG_IMAGES_PREFIX'] + filename + '.jpg')
    else:
        resized = resize_image(io.BytesIO(response.content), int(os.environ['IMAGE_WIDTH']), int(os.environ['IMAGE_HEIGHT']))
        return s3_client.upload_fileobj(resized, os.environ['S3_BUCKET'], os.environ['BG_IMAGES_PREFIX'] + filename + '.jpg')


# Uploads an image meta description to S3.
def upload_meta_to_s3(s3_client, data, filename):
    result = s3_client.put_object(
        Body=(bytes(json.dumps(data, indent=2).encode('UTF-8'))),
        Bucket=os.environ['S3_BUCKET'],
        Key=os.environ['BG_IMAGES_PREFIX'] + filename + '.json'
    )
    assert result['ResponseMetadata']['HTTPStatusCode'] == 200


# AWS Lamda handler method.
def lambda_handler(event, context):
    logger.info('Operation started')
    s3 = session.client('s3')
    request_url = build_request_url()
    json_data = get_random_images(request_url)
    if (json_data == False):
        raise Exception('Failed to get image data, aborting...')
    filtered_data = filter_images(json_data)
    for index, image_data in enumerate(filtered_data):
        logger.info('Processing image: {}'.format(index))
        upload_image_to_s3(s3, image_data['image_url'], str(index + 1), False)
        upload_image_to_s3(s3, image_data['thumbnail_url'], str(index + 1) + '_thumbnail', True)
        upload_meta_to_s3(s3, image_data['meta_data'], str(index + 1))
    logger.info('Operation complete')
    return True
