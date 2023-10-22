1. Set AWS credentials at ~/.aws/credentials
2. Download SSH private key at ~/.ssh/vockey.pem
3. Set the proper permissions for the key: `chmod og-rwx ~/.ssh/vockey.pem`
4. Install requirements: `pip install -r requirements.txt`
5. Install vim and less: `sudo apt update && apt install -y vim less`
6. Edit `inventory.yml` file and replace `nifi` host DNS with hadoop_master node's **public** DNS.
7. Install nifi: `ansible-playbook -i inventory.yml --key-file=~/.ssh/vockey.pem --user ec2-user install-nifi.yml`
8. Create an http tunnel to access to Nifi (replace by master's public DNS):
```
ssh -i ~/.ssh/vockey.pem -N -L 8080:ec2-3-87-82-66.compute-1.amazonaws.com:8080 ec2-user@ec2-3-87-82-66.compute-1.amazonaws.com
```
9. Access nifi at http://ec2-3-87-82-66.compute-1.amazonaws.com:8080 (replace by master's public DNS).
