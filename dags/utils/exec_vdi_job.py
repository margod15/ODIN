import os
import sys
import yaml
import logging
import subprocess
import paramiko
from scp import SCPClient
from pathlib import Path
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

today = datetime.now()
date_log = today.strftime('%Y%m%d')

download_path = Path(__file__).parent / "token_download"
download_path.mkdir(exist_ok=True)

log_path = Path(__file__).parent / "log" / f"reverse_transfer_{date_log}.log"
log_path.parent.mkdir(exist_ok=True)
destination_gcp = "gs://<bucket_name>/logs/"
gcp_wildcard_path = "gs://<bucket_name>/data/*"
staging_base_dir = "/data/txt/"

# Define parameters
username = os.getenv('stagging_uid')
password = os.getenv('stagging_pass')  
hostname = os.getenv('stagging_host')
 
def run_cmd(cmd, shell_flag=True):
    use_shell = False if isinstance(cmd, (list, tuple)) else shell_flag
    logging.info(f"Running command: {cmd}")
    
    si = None
    creationflags = 0
    
    if os.name == "nt":
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = subprocess.SW_HIDE
        creationflags |= getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
        creationflags |= 0x00000008

    proc = subprocess.run(
        cmd,
        shell=use_shell,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        startupinfo=si,
        creationflags=creationflags
    )

    if proc.stdout:
        logging.info(f"   stdout:\n{proc.stdout.strip()}")
    if proc.stderr:
        logging.error(f"   stderr:\n{proc.stderr.strip()}")
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()

def load_token(token_filename):
    token_path = download_path / token_filename
    with open(token_path, 'r') as f:
        return yaml.safe_load(f)

def execute_job(conf_token):
    job_name = conf_token['dag_name']
    logging.info(f"Starting Job execution {job_name}.")
    job_script = Path(Path(__file__).parent).parent / job_name / "scripts" / conf_token['starter_file']
    
    cmd = [sys.executable, str(job_script), "--dstart", conf_token['date_run']] + (["--custom_variable", conf_token['custom_variable']] if conf_token['custom_variable'] != 'null' else [])

    rc, out, err = run_cmd(cmd)
    if rc != 0:
        logging.error(f"Job `{job_name}` failed: {err}")
    else:
        logging.info(f"Job `{job_name}` succeeded\n{out}")
    return out

def process_job_run(token_file):
    try:
        logging.info(f"Processing token file: {token_file}")

        conf_token = load_token(token_file)
        execute_job(conf_token)
    except Exception as e:
        logging.error(f"Error processing {token_file}: {e}")
    
def remove_consumed_file(token_files):
    for filename in token_files:
        gcs_path = f"gs://<bucket_name>/data/{filename}"
        run_cmd(f'gsutil rm "{gcs_path}"')
        try:
            os.remove(download_path / filename)
            logging.info(f"Deleted local file: {filename}")
        except Exception as e:
            logging.error(f"Failed to delete local file {filename}: {e}")

def delete_local_file(ssh, file_path):
    ssh.exec_command(f"rm -f {file_path}")
 
def create_ssh_client(hostname, username, password):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, password=password)
    return ssh

def create_scp_client(hostname, username, password):
    ssh = create_ssh_client(hostname, username, password)
    return SCPClient(ssh.get_transport()), ssh

def download_from_gcp_to_staging(ssh, gcp_path, staging_path):
    cmd = f"gsutil -m cp {gcp_path} {staging_path}"
    stdin, stdout, stderr = ssh.exec_command(cmd)
    stdout.channel.recv_exit_status()
    out = stdout.read().decode()
    err = stderr.read().decode()
    if "CommandException" in err or "No URLs matched" in err:
         logging.error(f"Failed: GCP → Staging for {gcp_path}")
         return False
    logging.info(f"GCP → Staging success: {gcp_path}")
    return True

def transfer_local_to_staging(scp, remote_path, local_path):
    local_files = []

    project_root = Path(__file__).parent.parent
    log_files = list(project_root.glob("*/logs/*.log"))

    if not log_files:
        logging.info("No file found.")
        return

    for file_name in log_files:
        remote_path = f"{staging_base_dir}/{os.path.basename(file_name)}"
        scp.put(file_name, remote_path)
        logging.info(f"File {file_name} sent to staging.")
        transfer_to_gcp(ssh, remote_path, destination_gcp)
    scp.close()

def transfer_to_gcp(ssh, remote_path, target_gcp_path):
    command = f"gsutil cp {remote_path} {target_gcp_path}"
    stdin, stdout, stderr = ssh.exec_command(command)
    result = stdout.read().decode()
    error = stderr.read().decode()

    if "CommandException" in error or "No URLs matched" in error:
         logging.error(f"Failed: Staging → GCP for {target_gcp_path}")
         return False
    logging.info(f"Staging → GCP success: {target_gcp_path}")

def copy_from_staging_to_local(scp, remote_path, local_path):
    try:
        scp.get(remote_path, local_path=str(local_path))
        logging.info(f"Copied: {remote_path} → {local_path}")
        return True
    except Exception as e:
        logging.error(f"Failed SCP copy to local: {e}")
        return False
    
if __name__ == "__main__":
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="w"
    )
    logging.info("Starting full GCS → Staging → Local transfer and job execution...")

    try:
        scp, ssh = create_scp_client(hostname, username, password)

        if download_from_gcp_to_staging(ssh, gcp_wildcard_path, staging_base_dir):
            stdin, stdout, stderr = ssh.exec_command(f"ls {staging_base_dir}*.yaml")
            files = stdout.read().decode().splitlines()

            for remote_path in files:
                filename = os.path.basename(remote_path)
                local_path = download_path / filename
                success = copy_from_staging_to_local(scp, remote_path, local_path)
                if not success:
                    continue

            logging.info(f"Token files found locally: {[f.name for f in download_path.glob('*.yaml')]}")

            token_files = [f for f in os.listdir(download_path) if f.endswith(".yaml")]

            # Parallelism job run
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_job_run, token_file) for token_file in token_files]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(e)
        
            logging.info("All transfers and jobs completed successfully.")
            transfer_local_to_staging(scp, remote_path, local_path)
            
            remove_consumed_file(token_files)
            delete_local_file(ssh, remote_path)

    except Exception as e:
        logging.exception(f"Unhandled error: {e}")
        sys.exit(1)
    finally:
        try:
            scp.close()
            ssh.close()
        except:
            pass
        logging.info("SSH/SCP connections closed.")