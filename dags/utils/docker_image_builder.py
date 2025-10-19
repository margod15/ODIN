import hashlib
import tarfile
import io
from google.cloud import storage
from google.cloud import artifactregistry_v1

PROJECT_ID = "<project_id>"
REGION = "asia-southeast2"
BUILD_BUCKET = "<bucket_name>"
DAGS_BUCKET = "<bucket_name>"  
ARTIFACT_REPO = "<artifact_repo>"

def generate_image_tag(requirements_bytes):
    return hashlib.sha256(requirements_bytes).hexdigest()

def download_requirements_from_gcs(job_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(DAGS_BUCKET)
    job_path = f"dags/{job_name}/scripts/"
    requirements_blob = bucket.blob(job_path + f"requirements_{job_name}.txt")
    requirements_bytes = requirements_blob.download_as_bytes()
    return requirements_bytes, job_path

def build_image_uri(job_name, tag):
    image_uri = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/{job_name}:{tag}"
    cleanup_old_images(job_name, tag)
    return image_uri

def cleanup_old_images(job_name, latest_tag):
    client = artifactregistry_v1.ArtifactRegistryClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{ARTIFACT_REPO}"
    response = client.list_docker_images(request={"parent": parent})
    for image in response:
        if f"{job_name}:" not in image.uri:
            continue
        current_tag = image.uri.split(":")[-1]
        if current_tag != latest_tag:
            print(f"Removing old image: {image.uri}")
            client.delete_docker_image(name=image.name)

def check_image_exists(image_uri):
    client = artifactregistry_v1.ArtifactRegistryClient()
    parent = f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{ARTIFACT_REPO}"
    response = client.list_docker_images(request={"parent": parent})
    return any(image.uri == image_uri for image in response)

def package_and_upload_source(job_name):
    client = storage.Client()
    dags_bucket = client.bucket(DAGS_BUCKET)
    build_bucket = client.bucket(BUILD_BUCKET)

    source_prefix = f"dags/{job_name}/scripts/"
    output_blob_path = f"{job_name}/source.tar.gz"

    blobs = list(client.list_blobs(DAGS_BUCKET, prefix=source_prefix))
    tar_stream = io.BytesIO()

    with tarfile.open(fileobj=tar_stream, mode="w:gz") as tar:
        for blob in blobs:
            if blob.name.endswith("source.tar.gz"):
                continue
            blob_data = blob.download_as_bytes()
            arcname = blob.name[len(source_prefix):]
            tarinfo = tarfile.TarInfo(name=arcname)
            tarinfo.size = len(blob_data)
            tar.addfile(tarinfo, io.BytesIO(blob_data))

        dockerfile_content = generate_dockerfile(job_name)
        dockerfile_bytes = dockerfile_content.encode("utf-8")
        dockerfile_info = tarfile.TarInfo(name="Dockerfile")
        dockerfile_info.size = len(dockerfile_bytes)
        tar.addfile(dockerfile_info, io.BytesIO(dockerfile_bytes))

    tar_stream.seek(0)

    build_blob = build_bucket.blob(output_blob_path)
    build_blob.upload_from_file(tar_stream, content_type="application/gzip")
    print(f"Uploaded source package with Dockerfile: gs://{BUILD_BUCKET}/{output_blob_path}")

def generate_dockerfile(job_name):
    return f"""
        FROM python:3.11-slim

        WORKDIR /app

        COPY . /app

        RUN pip install --no-cache-dir -r requirements_{job_name}.txt

        ENTRYPOINT ["python"]
    """.strip()

def prepare_job(job_name):
    requirements_bytes, job_path = download_requirements_from_gcs(job_name)
    tag = generate_image_tag(requirements_bytes)
    image_uri = build_image_uri(job_name, tag)

    package_and_upload_source(job_name)

    build_config = {
        "steps": [
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": [
                    "build",
                    "-t", image_uri,
                    "."
                ]
            }
        ],
        "images": [
            image_uri
        ],
        "source": {
            "storage_source": {
                "bucket": BUILD_BUCKET,
                "object": f"{job_name}/source.tar.gz"
            }
        }
    }

    return {
        "image_uri": image_uri,
        "job_path": job_path,
        "tag": tag,
        "build_required": not check_image_exists(image_uri),
        "build_config": build_config
    }
