#!/bin/bash
# Run Denum's OFFICIAL CLI (unmodified, commit a3a6975) on one input file exactly as the README says,
# then decode with the OFFICIAL Python decompressor (Denum_python_package/decompress.py, unmodified),
# using only the files contained in the official per-block archive output/DATASET/compressed<i>.xz.
# usage: native_official.sh DATASET INPUT_FILE OUTDIR
set -u
D=<WORKDIR>/r76_additional_20260929/baselines/denum
DS=$1; IN=$2; OUT=$3
mkdir -p "$OUT" && cd "$OUT" || exit 1
rm -rf work dec
mkdir -p work/Logs/$DS work/output
cp "$IN" work/Logs/$DS/$DS.log
cd work
/usr/bin/time -v $D/bin/denum_compress $DS 100000 1 > ../encode.stdout 2> ../encode.stderr
echo "encode_exit=$?"
cd ..
ls -la work/output/$DS/ > encode_outputs.txt
nblk=$(ls work/output/$DS/compressed*.xz 2>/dev/null | wc -l)
echo "archives=$nblk bytes=$(cat work/output/$DS/compressed*.xz | wc -c)"
# ---- official python decoder: it iterates chunkID=1.. while ../Output/DS/<chunkID> exists and
#      reads the extracted archive members from ../decompress_output/DS/<chunkID>/ (kernel_decompress is
#      commented out upstream, so extraction is done here with tar; no member is added or changed).
mkdir -p dec
cp -r $D/src/Denum/Denum_python_package dec/
for ((i=0;i<nblk;i++)); do
  c=$((i+1))
  mkdir -p dec/Output/$DS/$c dec/decompress_output/$DS/$c dec/x$i
  tar -xJf work/output/$DS/compressed$i.xz -C dec/x$i
  mv dec/x$i/output/$DS/$i/* dec/decompress_output/$DS/$c/
  rmdir -p dec/x$i/output/$DS/$i 2>/dev/null
done
cd dec/Denum_python_package
PYTHONPATH=$D/pydeps /usr/bin/time -v python3 decompress.py $DS > ../../decode.stdout 2> ../../decode.stderr
echo "decode_exit=$?"
cd ../..
rm -f decoded.log
for ((i=0;i<nblk;i++)); do c=$((i+1)); cat dec/decompress_output/$DS/$c/Decompressed$DS.log >> decoded.log; done
echo "orig_sha=$(sha256sum < "$IN" | cut -c1-16) bytes=$(stat -c %s "$IN") lines=$(wc -l < "$IN")"
echo "dec_sha=$(sha256sum < decoded.log | cut -c1-16) bytes=$(stat -c %s decoded.log) lines=$(wc -l < decoded.log)"
cmp "$IN" decoded.log && echo BYTE_EXACT || echo NOT_BYTE_EXACT
