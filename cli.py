from typing import Optional
import boto3
import argparse
from dataclasses import dataclass, asdict
from pprint import pprint as pp
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client("s3")


@dataclass
class CopyTask:
    source_bucket: str
    source_key: str
    destination_bucket: str
    destination_key: str


@dataclass
class EnrichedCopyTask(CopyTask):
    """
    CopyTask enriched with the output of HeadObject S3 API call
    """

    total_bytes: int
    parts_count: int
    part_size_bytes: int


@dataclass
class UploadPartCopyTask(EnrichedCopyTask):
    """
    CopyTask enriched with the output of HeadObject S3 API call
    """

    part_number: int
    upload_id: str
    copy_source_range: str


def parse_content_range(s: str) -> int:
    """
    This parses the `content-range` header returned by the S3 HeadObject API when
    it is called with a PartNumber.
    It returns the start and end byte range along with the total number of bytes.

    >>> parse_content_range("bytes 0-8388607/20277918669")
    (0, 8388607, 20277918669)
    """

    unit, _, range_total = s.partition(" ")
    assert unit == "bytes"
    range, _, total = range_total.partition("/")
    start, _, end = range.partition("-")
    start, end, total = int(start), int(end), int(total)
    assert start <= end
    assert end <= total
    return start, end, total


def get_part_byte_ranges(task: EnrichedCopyTask):
    """
    Given an EnrichedCopyTask, calculate the byte ranges for each
    UploadPartCopy operation.
    """

    start = 0
    remaining = task.total_bytes
    for part_number in range(1, task.parts_count + 1):

        if remaining > task.part_size_bytes:
            current = task.part_size_bytes
        else:
            current = remaining

        end = start + current - 1
        yield (part_number, start, end)

        start += current
        remaining -= current


def make_upload_part_copy_tasks(task: EnrichedCopyTask, upload_id: str):
    return (
        UploadPartCopyTask(
            **asdict(task),
            part_number=part_number,
            upload_id=upload_id,
            copy_source_range=f"bytes={start}-{end}",
        )
        for part_number, start, end in get_part_byte_ranges(task)
    )


def upload_part_copy(task: UploadPartCopyTask):
    """
    This function wraps around s3.upload_part_copy and returns a dict
    in the format required by the CompleteMultipartUpload API.
    """

    response = s3.upload_part_copy(
        Bucket=task.destination_bucket,
        Key=task.destination_key,
        CopySource=f"{task.source_bucket}/{task.source_key}",
        CopySourceRange=task.copy_source_range,
        PartNumber=task.part_number,
        UploadId=task.upload_id,
    )

    return {"PartNumber": task.part_number, "ETag": response["CopyPartResult"]["ETag"]}


def multipart_upload(task: EnrichedCopyTask):
    """
    This function will create a multipart upload, upload all of the parts
    in parallel via threads, and then complete the multipart upload.

    There is no error handling to clean up and abort the multipart upload
    at this time.
    """

    response = s3.create_multipart_upload(
        Bucket=task.destination_bucket,
        Key=task.destination_key,
    )

    upload_id = response["UploadId"]
    upload_part_copy_tasks = make_upload_part_copy_tasks(task, upload_id)

    with ThreadPoolExecutor() as executor:
        parts = list(
            tqdm(
                executor.map(upload_part_copy, upload_part_copy_tasks),
                total=task.parts_count,
            )
        )

    response = s3.complete_multipart_upload(
        Bucket=task.destination_bucket,
        Key=task.destination_key,
        MultipartUpload={"Parts": parts},
        UploadId=upload_id,
    )

    print(response)

    return response


def copy(task):
    """
    This function determines whether to use regular copy vs multipart upload
    based on the original object that is being copied.

    If the original object was multipart uploaded, it will proceed with doing a
    multipart upload.

    It also assumes consistent part sizes i.e that the size of the first part,
    `PartNumber=1`, is the same for all parts (except, of course the last part).
    """

    response = s3.head_object(
        Bucket=task.source_bucket, Key=task.source_key, PartNumber=1
    )
    parts_count = response["PartsCount"] if "PartsCount" in response else 1
    content_range = response["ResponseMetadata"]["HTTPHeaders"]["content-range"]
    start, end, total = parse_content_range(content_range)
    part_size_bytes = end - start + 1
    task = EnrichedCopyTask(
        **asdict(task),
        parts_count=parts_count,
        part_size_bytes=part_size_bytes,
        total_bytes=total,
    )

    if task.parts_count == 1:
        raise NotImplementedError("regular copy not implemented")
    else:
        multipart_upload(task)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--source-key", required=True)
    parser.add_argument("--destination-bucket", required=True)
    parser.add_argument("--destination-key", required=True)
    args = parser.parse_args()

    task = CopyTask(
        source_bucket=args.source_bucket,
        source_key=args.source_key,
        destination_bucket=args.destination_bucket,
        destination_key=args.destination_key,
    )

    copy(task)


if __name__ == "__main__":
    main()
