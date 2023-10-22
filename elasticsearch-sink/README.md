1. Set AWS credentials at ~/.aws/credentials
2. Download SSH private key at ~/.ssh/vockey.pem
3. Set the proper permissions for the key: `chmod og-rwx ~/.ssh/vockey.pem`
4. Install requirements: `pip install -r requirements.txt`
5. Install vim and less: `sudo apt update && apt install -y vim less`
6. Edit `inventory.yml` file and replace each server **public** DNS with the current ones. For the elasticsearch_host node, you must use before created hadoop_master DNS.
7. Install elasticsearch: `ansible-playbook -i inventory.yml --key-file=~/.ssh/vockey.pem --user ec2-user install-elasticsearch.yml`
8. Connect to master instance and reset elasticsearch password: `/usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic`
9. Edit elasticsearch.yml file (`sudo nano /etc/elasticsearch/elasticsearch.yml`) and replace the following:
```
xpack.security.http.ssl:
  enabled: true
```
and
```
xpack.security.transport.ssl:
  enabled: true
```
by
```
xpack.security.http.ssl:
  enabled: false
```
and
```
xpack.security.transport.ssl:
  enabled: false
```
10. Restart elasticsearch: `sudo systemctl restart elasticsearch`
11. Create `weather-data` index: `curl -k -u "elastic:your-password" -X PUT "http://localhost:9200/weather-data?pretty"`
12. Once you have redirected the data from NiFi you can search the index like this: `curl -u "elastic:t-hN0uP7d*VR0dJMhPTZ" -X GET "http://localhost:9200/weather-data/_search?q=name:Gasteiz&pretty"`
