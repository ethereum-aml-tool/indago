cargo build --release

./target/release/v2 -a seniority -d tornado 
./target/release/v2 -a seniority -d known-addresses 
./target/release/v2 -a seniority -d combined 
gsutil -m cp -r /data/blacklist/seniority-* gs://indago/blacklist/seniority

./target/release/v2 -a haircut -d tornado
./target/release/v2 -a haircut -d known-addresses
./target/release/v2 -a haircut -d combined
gsutil -m cp -r /data/blacklist/haircut-* gs://indago/blacklist/haircut