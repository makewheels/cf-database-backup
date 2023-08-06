import os
import pymongo
import json
from bson import json_util
import tarfile
import datetime
import time
import humanize
import shutil
import oss2


def connect_to_mongodb(url, port, username, password):
    if password is None or password == '':
        client = pymongo.MongoClient(f"mongodb://{url}:{port}/")
    else:
        client = pymongo.MongoClient(f"mongodb://{username}:{password}@{url}:{port}/")
    return client


def backup_mongodb(client, database_name, output_path):
    db = client[database_name]
    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        cursor = collection.find()

        collection_size = 0  # 用于统计集合大小
        collection_path = os.path.join(output_path, f'{collection_name}.json')
        with open(collection_path, 'w') as f:
            for document in cursor:
                f.write(json.dumps(document, default=json_util.default) + '\n')
                collection_size += len(json.dumps(document, default=json_util.default))

        print(
            f"备份集合 {collection_name}，文档数量：{collection.estimated_document_count()}，大小：{humanize.naturalsize(collection_size)}")


def create_backup_archive(backup_file_path):
    backup_archive = f'{backup_file_path}.tgz'
    with tarfile.open(backup_archive, 'w:gz') as tar:
        for file_name in os.listdir(backup_file_path):
            file_path = os.path.join(backup_file_path, file_name)
            tar.add(file_path, arcname=file_name)
    return backup_archive


def upload_to_oss(backup_archive, object_key):
    auth = oss2.Auth(os.environ.get('OSS_ACCESS_KEY_ID'), os.environ.get('OSS_ACCESS_KEY_SECRET'))
    bucket = oss2.Bucket(auth, os.environ.get('OSS_ENDPOINT'), os.environ.get('OSS_BUCKET_NAME'))
    with open(backup_archive, 'rb') as f:
        bucket.put_object(object_key, f)
    print(f"文件上传到 OSS 成功，对象键：{object_key}")


def main():
    # 设置 MongoDB 连接信息
    mongodb_url = 'localhost'
    mongodb_port = 27017
    mongodb_username = 'root'
    mongodb_password = ''
    database_name = 'video-2022'

    # 打印 MongoDB 连接信息
    mongodb_info = {
        "MongoDB_URL": mongodb_url,
        "Port": mongodb_port,
        "Username": mongodb_username,
        "Database Name": database_name
    }
    print(f"MongoDB 连接信息：{json.dumps(mongodb_info)}")

    # 连接 MongoDB
    mongodb_client = connect_to_mongodb(mongodb_url, mongodb_port, mongodb_username, mongodb_password)

    # 创建备份目录
    backup_date = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    backup_file_path = f'D:/{database_name}_{backup_date}'
    os.makedirs(backup_file_path, exist_ok=True)

    # 记录程序开始时间
    start_time = time.time()

    # 执行 MongoDB 备份
    backup_mongodb(mongodb_client, database_name, backup_file_path)

    # 打印原始大小、压缩后大小和压缩比例
    total_original_size = sum(os.path.getsize(os.path.join(backup_file_path, f)) for f in os.listdir(backup_file_path))
    backup_archive = create_backup_archive(backup_file_path)
    compressed_size = os.path.getsize(backup_archive)
    compression_ratio = (total_original_size - compressed_size) / total_original_size * 100
    print(
        f"大小：{humanize.naturalsize(total_original_size)} -> {humanize.naturalsize(compressed_size)}，压缩比例：{compression_ratio:.2f}%")

    # 删除原始 JSON 文件和文件夹
    shutil.rmtree(backup_file_path)
    print(f"删除原始 JSON 文件和文件夹：{backup_file_path}")

    # 上传备份文件到阿里云 OSS
    object_key = os.path.basename(backup_archive)
    upload_to_oss(backup_archive, object_key)

    # 打印程序运行时间
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n备份完成，总运行时间：{elapsed_time:.2f} 秒")


if __name__ == '__main__':
    main()
