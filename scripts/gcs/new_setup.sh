mkdir data
git clone https://github.com/ethereum-aml-tool/indago.git
echo 'export PYTHONPATH=$PYTHONPATH:~/indago' >> ~/.bashrc
source ~/.bashrc
cd indago
sudo apt install python3-pip
pip install --no-cache-dir --upgrade -r requirements.txt