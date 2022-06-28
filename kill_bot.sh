kill -9 $(ps -ax | grep -e run.py | awk 'NR==1{print $1}')
