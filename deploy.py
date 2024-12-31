import os
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient

def deploy_directory(local_directory, remote_host, remote_user, remote_password, remote_directory):
    # Ensure the local directory exists
    if not os.path.isdir(local_directory):
        raise ValueError(f"Local directory {local_directory} does not exist.")

    # Connect to the remote machine
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    try:
        ssh.connect(remote_host, username=remote_user, password=remote_password)
        print("Connected to remote host")

        # Create the target directory on the remote machine
        stdin, stdout, stderr = ssh.exec_command(f'mkdir -p {remote_directory}')
        stdout.channel.recv_exit_status()  # Wait for command to complete

        # Use SCP to transfer files
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_directory, remote_path=remote_directory, recursive=True)
            print(f"Successfully transferred {local_directory} to {remote_directory} on {remote_host}")

    except Exception as e:
        print(f"Failed to deploy directory: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    # Get the directory containing this script
    script_directory = os.path.dirname(os.path.abspath(__file__))

    # Remote configuration
    REMOTE_HOST = "gwngames.com"  # Replace with your remote server's address
    REMOTE_USER = ""  # Replace with your username
    REMOTE_PASSWORD = ""  # Replace with your password
    REMOTE_DIRECTORY = "/opt/gpub/viewer"  # Replace with your target directory on the remote machine

    # Deploy the script's directory
    deploy_directory(script_directory, REMOTE_HOST, REMOTE_USER, REMOTE_PASSWORD, REMOTE_DIRECTORY)
