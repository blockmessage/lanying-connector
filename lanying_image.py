from PIL import Image, ImageFile
import logging
import math
import base64
from io import BytesIO
import requests
import time
import lanying_utils
import lanying_redis

# Only probe headers for dimensions; never download an unbounded image body.
IMAGE_PROBE_MAX_BYTES = 4 * 1024 * 1024
IMAGE_PROBE_TIMEOUT = (3, 5)
IMAGE_PROBE_SECONDS = 10

def calculate_tokens(image_path, detail='auto'):
    if detail == 'low':
        return 85
    # 获取图像尺寸
    width, height = get_image_size_with_cache(image_path)

    # 根据规则判断细节参数是否为自动
    if detail == 'auto':
        # 根据图像尺寸判断使用低分辨率还是高分辨率模式
        if width > 100 or height > 100:
            detail = 'high'
        else:
            detail = 'low'

    # 根据细节参数计算 tokens 数量
    if detail == 'low':
        tokens = 85
    elif detail == 'high':
        new_width,new_height = resize(width, height)
        
        # 计算图像由多少个 512px 方形组成
        num_tiles = math.ceil(new_width / 512) * math.ceil(new_height / 512)
        
        # 计算总 tokens 数量
        tokens = 170 * num_tiles + 85

    logging.info("calculate_image_tokens | width:%s, height:%s, detail:%s, tokens:%s",
                 width, height, detail, tokens)
    return tokens

def resize(width, height):
    if max(width, height) > 2048:
        if width > height:
            new_width = 2048
            new_height = int(height * (2048 / width))
        else:
            new_height = 2048
            new_width = int(width * (2048 / height))
    else:
        new_width = width
        new_height = height

    # 将最短边缩放为 768px
    if min(new_width, new_height) > 768:
        if new_width < new_height:
            new_width = 768
            new_height = int(height * (768 / width))
        else:
            new_height = 768
            new_width = int(width * (768 / height))
    return new_width, new_height

def get_image_size_with_cache(url_or_base64):
    key = get_image_size_cache_key(url_or_base64)
    redis = lanying_redis.get_redis_connection()
    info = lanying_redis.redis_get(redis, key)
    if info is None:
        width,height = get_image_size(url_or_base64)
        redis.setex(key, 1800, f"{width},{height}")
        return width,height
    fields = info.split(',')
    width = int(fields[0])
    height = int(fields[1])
    return width, height

def get_image_size_cache_key(url_or_base64):
    if url_or_base64.startswith("data:image"):
        value = lanying_utils.sha256(url_or_base64)
    else:
        value = url_or_base64
    return f"lanying-connector:image_size:{value}"

def _probe_image_size(chunks, started):
    parser = ImageFile.Parser()
    received = 0
    try:
        for chunk in chunks:
            if time.monotonic() - started > IMAGE_PROBE_SECONDS:
                raise TimeoutError('image dimension probe timed out')
            received += len(chunk)
            if received > IMAGE_PROBE_MAX_BYTES:
                raise ValueError('image dimension probe exceeded size limit')
            parser.feed(chunk)
            if parser.image is not None:
                return parser.image.size
        raise ValueError('image dimensions unavailable')
    finally:
        if parser.image is not None:
            parser.image.close()


def get_image_size(url_or_base64):
    try:
        started = time.monotonic()
        if url_or_base64.startswith("data:image"):
            # Decode only a bounded prefix; full pixel data isn't needed.
            start = url_or_base64.index(',') + 1
            encoded_limit = (IMAGE_PROBE_MAX_BYTES // 3) * 4
            image_data = base64.b64decode(url_or_base64[start:start + encoded_limit])
            return _probe_image_size([image_data], started)
        else:
            with requests.get(url_or_base64, stream=True, timeout=IMAGE_PROBE_TIMEOUT) as response:
                response.raise_for_status()
                return _probe_image_size(response.iter_content(chunk_size=8192), started)
    except Exception as e:
        # Preserve the existing size estimate on failure. Signed URLs, image
        # bodies and request exception text must not enter logs.
        logging.warning("image dimension probe failed; using estimated size | error_type:%s",
                        type(e).__name__)
        return 768, 2048

def encode_image(image_path):
  with open(image_path, "rb") as image_file:
    base64_image = base64.b64encode(image_file.read()).decode('utf-8')
    return f"data:image/jpeg;base64,{base64_image}"

def create_mask_image(transparency_area, image_size):
    # 创建一个新的空白图像，大小与原始图像相同，初始化为完全不透明
    mask_image = Image.new("RGBA", image_size, (255, 255, 255, 255))

    # 获取透明区域的位置和大小
    x_percent = transparency_area.get("x_percent", 0)
    y_percent = transparency_area.get("y_percent", 0)
    width_percent = transparency_area.get("width_percent", 0)
    height_percent = transparency_area.get("height_percent", 0)
    if width_percent == 0:
        width_percent = 100
    if height_percent == 0:
        height_percent = 100

    # 计算透明区域在图像中的具体位置
    x = int(image_size[0] * x_percent / 100)
    y = int(image_size[1] * y_percent / 100)
    width = int(image_size[0] * width_percent / 100)
    height = int(image_size[1] * height_percent / 100)

    # 将透明区域设置为透明
    mask_image.paste((0, 0, 0, 0), (x, y, x + width, y + height))

    return mask_image

def make_png_image_and_mask(image_path, transparency_area, max_dimension=1024):
    try:
        logging.info(f"make_png_image_and_mask | image_path:{image_path}")
        input_image = Image.open(image_path)
        width, height = input_image.size
        if width > max_dimension or height > max_dimension:
            if width > height:
                new_width = max_dimension
                new_height = int(height * (max_dimension / width))
            else:
                new_height = max_dimension
                new_width = int(width * (max_dimension / height))
            input_image = input_image.resize((new_width, new_height), Image.ANTIALIAS)
        rgba_image = input_image.convert("RGBA")

        # 创建一个新的空白图像，大小与原始图像相同，初始化为完全不透明
        png_image = Image.new("RGBA", rgba_image.size, (255, 255, 255, 255))

        # 将原始图像合成到新图像中，保留透明通道
        png_image.paste(rgba_image, (0, 0), rgba_image)
        mask_image = create_mask_image(transparency_area, png_image.size)
        png_image.save(image_path + ".resize.png")
        mask_image.save(image_path + ".mask.png")
        return {'result': 'ok', 'png_image': png_image, 'mask_image': mask_image}
    except Exception as e:
        logging.exception(e)
        logging.info(f"fail to make_png_image_and_mask | image_path:{image_path}, transparency_area:{transparency_area}, max_dimension:{max_dimension}")
        return {'result': 'error', 'message': 'fail to transform image'}

def image_to_byte_io(image):
    image_bytes = BytesIO()
    image.save(image_bytes, format='PNG')
    image_bytes.seek(0)
    return image_bytes

def png_to_jpg(png_filename, jpg_filename):
    png_image = Image.open(png_filename)
    png_image.convert('RGB').save(jpg_filename, quality=85)
