cargo build --release

./target/release/v2 -a seniority -d tornado 
# ./target/release/v2 -a seniority -d known-addresses 

gsutil -m cp -r /data/blacklist/seniority-* gs://indago/blacklist/seniority