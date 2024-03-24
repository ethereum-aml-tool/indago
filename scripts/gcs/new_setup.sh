mkdir data
mkdir data/traces
sudo apt update
echo 'export PYTHONPATH=$PYTHONPATH:~/indago' >> ~/.bashrc
source ~/.bashrc
cd indago
sudo apt install python3-pip -y
pip install --no-cache-dir --upgrade -r requirements.txt

# git clone https://github.com/ethereum-aml-tool/indago.git
# nohup python3 scripts/gcs/download_and_sort.py &> /home/ponbac/out.txt &