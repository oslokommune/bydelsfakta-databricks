#!/usr/bin/env bash

# Create upload directories inside the uploads Volume.
#
# Usage:
#   ./scripts/create_upload_dirs.sh <catalog> <schema>
#
# Example:
#   ./scripts/create_upload_dirs.sh dig_bydelsfakta_dev_green bronze_default

set -euo pipefail

catalog="${1:?Usage: $0 <catalog> <schema>}"
schema="${2:?Usage: $0 <catalog> <schema>}"
base="/Volumes/${catalog}/${schema}/uploads"

dirs=(
    befolkning-etter-kjonn-og-alder
    boligmengde-etter-boligtype
    boligpriser-blokkleiligheter
    botid
    botid-ikke-vestlige
    dode
    dodsrater
    eieform
    fattige-husholdninger
    flytting-fra-etter-alder
    flytting-til-etter-alder
    fodte
    husholdninger-etter-rom-per-person
    husholdninger-med-barn
    husholdningstyper
    innvandrer-befolkningen-0-15-ar
    kommunale-boliger
    landbakgrunn-storste-innvandringsgrupper
    lav-utdanning
    median-totalpris
    neets
    redusert-funksjonsevne
    sysselsatte
    trangbodde
    utdanning
)

for dir in "${dirs[@]}"; do
    echo "Creating ${base}/${dir}/"
    databricks api put "/api/2.0/fs/directories${base}/${dir}" -p BYDELSFAKTA
done

echo "Done. Created ${#dirs[@]} directories."
