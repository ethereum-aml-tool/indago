cargo build --release

./target/release/v2 -a poison -d tornado 
./target/release/v2 -a poison -d known-addresses 

gsutil -m cp -r /data/blacklist/poison-* gs://indago/blacklist/poison